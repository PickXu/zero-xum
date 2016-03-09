/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

// CS TODO: this has to come before sm_base because w_base.h defines
// a macro called "align", which is probably the name of a function
// or something inside boost regex
#include <boost/regex.hpp>

#define SM_SOURCE
#define LOG_STORAGE_C

#include "sm_base.h"
#include "chkpt.h"

#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <os_interface.h>
#include <atomic>
#include <thread>
#include <chrono>

#include "log_storage.h"
#include "log_core.h"
#include "srwlock.h"

// needed for skip_log (TODO fix this)
#include "logdef_gen.cpp"

typedef smlevel_0::fileoff_t fileoff_t;
const string log_storage::log_prefix = "log.";
const string log_storage::log_regex = "log\\.[1-9][0-9]*";

class partition_recycler_t : public smthread_t
{
public:
    partition_recycler_t(log_storage* storage)
        : smthread_t(t_regular, "partition_recycler"), storage(storage),
        retire(false)
    {}

    virtual ~partition_recycler_t() {}

    void run()
    {
        while (!retire) {
            unique_lock<mutex> lck(storage->_recycler_mutex);
            storage->_recycler_condvar.wait(lck);
            storage->delete_old_partitions();
        }
    }

    log_storage* storage;
    std::atomic<bool> retire;
};

/*
 * Opens log files in logdir and initializes partitions as well as the
 * given LSN's. The buffer given in prime_buf is primed with the contents
 * found in the last block of the last partition -- this logic was moved
 * from the various prime methods of the old log_core.
 */
log_storage::log_storage(const sm_options& options)
    :
        _skip_log(new skip_log)
{
    std::string logdir = options.get_string_option("sm_logdir", "");
    if (logdir.empty()) {
        cerr << "ERROR: sm_logdir must be set to enable logging." << endl;
        W_FATAL(eCRASH);
    }
    _logpath = logdir;

    bool reformat = options.get_bool_option("sm_format", false);

    if (!reformat && !fs::exists(_logpath)) {
        cerr << "Error: could not open the log directory " << logdir <<endl;
        W_COERCE(RC(eOS));
    }

    fileoff_t psize = fileoff_t(options.get_int_option("sm_log_partition_size", 1024));
    // option given in MB -> convert to B
    psize = psize * 1024 * 1024;
    // round to next multiple of the log buffer segment size
    psize = (psize / log_core::SEGMENT_SIZE) * log_core::SEGMENT_SIZE;
    _partition_size = psize;

    // maximum number of partitions on the filesystem
    _max_partitions = options.get_int_option("sm_log_max_partitions", 0);

    partition_number_t  last_partition = 1;

    fs::directory_iterator it(_logpath), eod;
    boost::regex rx(log_regex, boost::regex::basic);
    for (; it != eod; it++) {
        fs::path fpath = it->path();
        string fname = fpath.filename().string();

        if (boost::regex_match(fname, rx)) {
            if (reformat) {
                fs::remove(fpath);
                continue;
            }

            long pnum = std::stoi(fname.substr(log_prefix.length()));
            _partitions[pnum] = make_shared<partition_t>(this, pnum);

            if (pnum >= last_partition) {
                last_partition = pnum;
            }
        }
        else {
            cerr << "log_storage: cannot parse filename " << fname << endl;
            W_FATAL(fcINTERNAL);
        }

    }



    auto p = get_partition(last_partition);
    if (!p) {
        create_partition(last_partition);
        p = get_partition(last_partition);
        w_assert0(p);
    }

    W_COERCE(p->open_for_append());
    _curr_partition = p;

    if(!p) {
        cerr << "ERROR: could not open log file for partition "
            << last_partition << endl;
        W_FATAL(eINTERNAL);
    }

    w_assert3(p->num() == last_partition);
}

log_storage::~log_storage()
{
    spinlock_write_critical_section cs(&_partition_map_latch);

    partition_map_t::iterator it = _partitions.begin();
    while (it != _partitions.end()) {
        auto p = it->second;
        p->close_for_read();
        p->close_for_append();
        it++;
    }

    _partitions.clear();

    delete _skip_log;

    if (_recycler_thread) {
        _recycler_thread->retire = true;
        _recycler_thread->join();
    }
}

shared_ptr<partition_t> log_storage::get_partition_for_flush(lsn_t start_lsn,
        long start1, long end1, long start2, long end2)
{
    w_assert1(end1 >= start1);
    w_assert1(end2 >= start2);
    // time to open a new partition? (used to be in log_core::insert,
    // now called by log flush daemon)
    // This will open a new file when the given start_lsn has a
    // different file() portion from the current partition()'s
    // partition number, so the start_lsn is the clue.
    auto p = curr_partition();
    if(start_lsn.file() != p->num()) {
        partition_number_t n = p->num();
        w_assert3(start_lsn.file() == n+1);
        w_assert3(n != 0);

        {
            W_COERCE(p->close_for_append());
            p = create_partition(n+1);
            W_COERCE(p->open_for_append());
        }
    }

    return p;
}

shared_ptr<partition_t> log_storage::get_partition(partition_number_t n) const
{
    spinlock_read_critical_section cs(&_partition_map_latch);
    partition_map_t::const_iterator it = _partitions.find(n);
    if (it == _partitions.end()) { return nullptr; }
    return it->second;
}

/*********************************************************************
 *
 *  log_storage::close_min(n)
 *
 *  Close the partition with the smallest index(num) or an unused
 *  partition, and
 *  return a ptr to the partition
 *
 *  The argument n is the partition number for which we are going
 *  to use the free partition.
 *
 *********************************************************************/
// CS TODO: disabled for now because we are supporting an unbouded
// number of partitions -- bounded list & recycling will be implemented later
// MUTEX: partition
#if 0
partition_t        *
log_storage::_close_min(partition_number_t n)
{
    // kick the cleaner thread(s)
    //if(smlevel_0::bf) smlevel_0::bf->wakeup_cleaners();

    /*
     *  If a free partition exists, return it.
     */

    /*
     * first try the slot that is n % PARTITION_COUNT
     * That one should be free.
     */
    int tries=0;
 again:
    partition_index_t    i =  (int)((n-1) % PARTITION_COUNT);
    partition_number_t   min = min_chkpt_rec_lsn().hi();
    partition_t         *victim;

    victim = _partition(i);
    if((victim->num() == 0)  ||
        (victim->num() < min)) {
        // found one -- doesn't matter if it's the "lowest"
        // but it should be
    } else {
        victim = 0;
    }

    if (victim)  {
        w_assert3( victim->index() == (partition_index_t)((n-1) % PARTITION_COUNT));
    }
    /*
     *  victim is the chosen victim partition.
     */
    if(!victim) {
        /*
         * uh-oh, no space left. Kick the page cleaners, wait a bit, and
         * try again. Do this no more than 8 times.
         *
         */
        {
            w_ostrstream msg;
            msg << "Thread " << me()->id << " "
            << "Out of log space  ("
            //<< space_left()
            << "); No empty partitions."
            << endl;
            fprintf(stderr, "%s\n", msg.c_str());
        }

        if(tries++ > 8) W_FATAL(eOUTOFLOGSPACE);
        //if(smlevel_0::bf) smlevel_0::bf->wakeup_cleaners();
        me()->sleep(1000);
        goto again;
    }
    w_assert1(victim);
    // num could be 0

    /*
     *  Close it.
     */
    if(victim->exists()) {
        /*
         * Cannot close it if we need it for recovery.
         */
        if(victim->num() >= min_chkpt_rec_lsn().hi()) {
            w_ostrstream msg;
            msg << " Cannot close min partition -- still in use!" << endl;
            // not mt-safe
            cerr  << msg.c_str() << endl;
        }
        w_assert1(victim->num() < min_chkpt_rec_lsn().hi());

        victim->close(true);
        victim->destroy();

    } else {
        w_assert3(! victim->is_open_for_append());
        w_assert3(! victim->is_open_for_read());
    }

    victim->clear();

    return victim;
}
#endif
shared_ptr<partition_t> log_storage::create_partition(partition_number_t pnum)
{
#if W_DEBUG_LEVEL > 2
    // No other partition may be open for append
    {
        spinlock_read_critical_section cs(&_partition_map_latch);
        partition_map_t::iterator it = _partitions.begin();
        for (; it != _partitions.end(); it++) {
            w_assert3(!it->second->is_open_for_append());
        }
    }
#endif

    // we should also free up if necessary, as done in close_min
    auto p = get_partition(pnum);
    if (p) {
        W_FATAL_MSG(eINTERNAL, << "Partition " << pnum << " already exists");
    }

    p = make_shared<partition_t>(this, pnum);
    p->set_size(0);

    w_assert3(_partitions.find(pnum) == _partitions.end());

    {
        // Add partition to map but only exit function once it has been
        // reduced to _max_partitions
        spinlock_write_critical_section cs(&_partition_map_latch);
        w_assert1(!_curr_partition || _curr_partition->num() == pnum - 1);
        _partitions[pnum] = p;
        _curr_partition = p;
    }

    // take checkpoint & kick-off partition recycler (oportunistically)
    if (_max_partitions > 0) {
        if (smlevel_0::chkpt) { smlevel_0::chkpt->wakeup_thread(); }
        if (smlevel_0::bf && smlevel_0::bf->get_cleaner()) {
            smlevel_0::bf->get_cleaner()->wakeup_cleaner();
        }
    }
    wakeup_recycler();

    // The check below does not require the mutex
    if (_max_partitions > 0 && _partitions.size() > _max_partitions) {
        // Log full! Try to clean-up old partitions.
        try_delete(pnum);
    }

    return p;
}

void log_storage::wakeup_recycler()
{
    if (!_recycler_thread) {
        _recycler_thread.reset(new partition_recycler_t(this));
        _recycler_thread->fork();
    }
    _recycler_condvar.notify_one();
}

unsigned log_storage::delete_old_partitions(partition_number_t older_than)
{
    if (older_than == 0) {
        lsn_t min_lsn = smlevel_0::chkpt->get_min_active_lsn();
        older_than = min_lsn.hi();
    }
    // CS TODO: talk to log archiver!

    list<shared_ptr<partition_t>> to_be_deleted;

    {
        spinlock_write_critical_section cs(&_partition_map_latch);

        partition_map_t::iterator it = _partitions.begin();
        while (it != _partitions.end()) {
            if (it->first < older_than) {
                to_be_deleted.push_front(it->second);
                it = _partitions.erase(it);
            }
            else { it++; }
        }
    }

    // Waint until the partitions to be deleted are not referenced anymore
    while (to_be_deleted.size() > 0) {
        auto p = to_be_deleted.front();
        to_be_deleted.pop_front();
        while (!p.unique()) {
            std::this_thread::sleep_for(chrono::milliseconds(1));
        }
        // Now this partition is owned exclusively by me.  Other threads cannot
        // increment reference counters because objects were removed from map,
        // and the critical section above guarantees visibility.
        p->destroy();
    }

    return to_be_deleted.size();
}

shared_ptr<partition_t> log_storage::curr_partition() const
{
    spinlock_read_critical_section cs(&_partition_map_latch);
    return _curr_partition;
}

string log_storage::make_log_name(partition_number_t pnum) const
{
    return make_log_path(pnum).string();
}

fs::path log_storage::make_log_path(partition_number_t pnum) const
{
    return _logpath / fs::path(log_prefix + to_string(pnum));
}

void log_storage::try_delete(partition_number_t pnum)
{
    /*
     * Log full -- we must delete a partition before continuing.  But we can't
     * invoke normal checkpoint & cleaner because they will attempt to generate
     * log records and block as well.  First we check if the oldest active
     * transaction (as known by the last checkpoint) has its begin in the
     * oldest partition file. If that's true, then no partition can be deleted
     * and we are stuck -- in other words, the log is "wedged". To avoid this,
     * a log space reservations scheme is required, but since we removed the
     * old and messy scheme, we must fail here. Since this is a research
     * prototype and this is quite a corner case, we don't worry too much about
     * it.
     */
    lsn_t min_xct_lsn = smlevel_0::chkpt->get_min_xct_lsn();
    if (min_xct_lsn.hi() == pnum - _max_partitions) {
        throw runtime_error("Log wedged! Cannot recycle partitions due to \
                old active transaction");
    }

    /*
     * Now check if any dirty page rec_lsn is in the oldest partition. If
     * that's true, then we're also stuck like above, because our cleaning
     * & checkpoint mechanisms require generating log records. We could simply
     * force all dirty pages from the buffer pool without generating log
     * records -- that would mean that those older log records would not be
     * required for recovery. However, the log analysis logic would not know
     * that without page_write log records. Again, it seems like the solution
     * is to have a reservation scheme, where enough log space is always reserved
     * for a full page cleaner round (e.g., one logrec for each frame)
     */
    lsn_t min_rec_lsn = smlevel_0::chkpt->get_min_rec_lsn();
    if (min_rec_lsn.hi() == pnum - _max_partitions) {
        throw runtime_error("Log wedged! Cannot recycle partitions due to \
                old dirty pages");
    }

    /*
     * Once we get here, we must be able to delete at least one partition
     * CS-TODO: there's potentially a deadlock here, since
     * delete_old_partitions will wait until the partition's shared_ptr has no
     * other references -- if a thread is holding a reference but waiting to
     * insert something in the full log, we get stuck.
     */
    unsigned deleted = delete_old_partitions();
    if (deleted == 0) {
        throw runtime_error("Log wedged! Cannot recycle partitions with \
                the available checkpoint information. Try increasing \
                max_partitions or partition_size.");
    }
}
