/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-MT -- Multi-threaded port of the SHORE storage manager

                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne

                         All Rights Reserved.

   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/*<std-header orig-src='shore'>

 $Id: log_core.cpp,v 1.20 2010/12/08 17:37:42 nhall Exp $

SHORE -- Scalable Heterogeneous Object REpository

Copyright (c) 1994-99 Computer Sciences Department, University of
                      Wisconsin -- Madison
All Rights Reserved.

Permission to use, copy, modify and distribute this software and its
documentation is hereby granted, provided that both the copyright
notice and this permission notice appear in all copies of the
software, derivative works or modified versions, and any portions
thereof, and that both notices appear in supporting documentation.

THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY
OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS
"AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND
FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

This software was developed with support by the Advanced Research
Project Agency, ARPA order number 018 (formerly 8230), monitored by
the U.S. Army Research Laboratory under contract DAAB07-91-C-Q518.
Further funding for this work was provided by DARPA through
Rome Research Laboratory Contract No. F30602-97-2-0247.

*/

#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#define SM_SOURCE
#define LOG_CORE_C

#include "sm_options.h"
#include "sm_base.h"
#include "logdef_gen.cpp"
#include "logtype_gen.h"
#include "logrec.h"
//#include "log.h"
#include "log_core.h"
#include "log_carray.h"
#include "log_lsn_tracker.h"
#include "eventlog.h"

#include "bf_tree.h"

#include "fixable_page_h.h"

#include <algorithm>
#include <sstream>
#include <w_strstream.h>
#include <sys/stat.h>

typedef smlevel_0::fileoff_t fileoff_t;



class ticker_thread_t : public smthread_t
{
public:
    ticker_thread_t(bool msec = false)
        : smthread_t(t_regular, "ticker"), msec(msec)
    {
        interval_usec = 1000; // 1ms
        if (!msec) {
            interval_usec *= 1000;
        }
        stop = false;
    }

    virtual ~ticker_thread_t() {}

    void shutdown()
    {
        stop = true;
        lintel::atomic_thread_fence(lintel::memory_order_release);
    }

    void run()
    {
        while (true) {
            lintel::atomic_thread_fence(lintel::memory_order_acquire);
            if (stop) {
                return;
            }
            ::usleep(interval_usec);
            if (msec) {
                sysevent::log(logrec_t::t_tick_msec);
            }
            else {
                sysevent::log(logrec_t::t_tick_sec);
            }
        }
    }

private:
    int interval_usec;
    bool msec;
    bool stop;
    // 80 bytes is enough to hold ticker logrec
    char lrbuf[80];
};

class fetch_buffer_loader_t : public smthread_t {
public:
    fetch_buffer_loader_t(log_core* log)
        : smthread_t(t_regular, "fetchbuf_loader"), log(log)
    {
    }

    virtual ~fetch_buffer_loader_t() {}

    void run()
    {
        W_COERCE(log->load_fetch_buffers());
    }

private:
    log_core* log;
};


class flush_daemon_thread_t : public smthread_t {
    log_core* _log;
public:
    flush_daemon_thread_t(log_core* log) :
         smthread_t(t_regular, "flush_daemon", WAIT_NOT_USED), _log(log) { }

    virtual void run() { _log->flush_daemon(); }
};

/*********************************************************************
 *
 *  log_core::fetch(lsn, rec, nxt, forward)
 *
 *  used in rollback and log_i
 *
 *  Fetch a record at lsn, and return it in rec. Optionally, return
 *  the lsn of the next/previous record in nxt.  The lsn parameter also returns
 *  the lsn of the log record actually fetched.  This is necessary
 *  since it is possible while scanning to specify an lsn
 *  that points to the end of a log file and therefore is actually
 *  the first log record in the next file.
 *
 * NOTE: caller must call release()
 *********************************************************************/

rc_t
log_core::fetch(lsn_t& ll, void* buf, lsn_t* nxt, const bool forward)
{
    INC_TSTAT(log_fetches);

    lintel::atomic_thread_fence(lintel::memory_order_acquire);
    if (ll < _fetch_buf_end && ll >= _fetch_buf_begin)
    {
        // log record can be found in fetch buffer -- no I/O
        size_t i = ll.hi() - _fetch_buf_first;
        if (_fetch_buffers[i]) {
            logrec_t* rp = (logrec_t*) (_fetch_buffers[i] + ll.lo());
            w_assert0(rp->valid_header(ll));

            if (rp->type() == logrec_t::t_skip)
            {
                if (forward) {
                    ll = lsn_t(ll.hi() + 1, 0);
                    return fetch(ll, buf, nxt, forward);
                }
                else { // backward scan
                    ll = *((lsn_t*) (_fetch_buffers[i] + ll.lo() - sizeof(lsn_t)));
                }

                rp = (logrec_t*) (_fetch_buffers[i] + ll.lo());
                w_assert0(rp->valid_header(ll));
            }

            if (nxt) {
                if (!forward && ll.lo() == 0) {
                    auto p = _storage->get_partition(ll.hi() - 1);
                    *nxt = p ? lsn_t(p->num(), p->get_size()) : lsn_t::null;
                }
                else {
                    if (forward) {
                        *nxt = ll;
                        nxt->advance(rp->length());
                    }
                    else {
                        memcpy(nxt, (char*) rp - sizeof(lsn_t), sizeof(lsn_t));
                    }
                }
            }

            memcpy(buf, rp, rp->length());

            return RCOK;
        }
    }

    if (forward && ll >= durable_lsn()) {
        w_assert0(ll == durable_lsn());
        // reading the durable_lsn during recovery yields a skip log record,
        return RC(eEOF);
    }
    if (!forward && ll == lsn_t::null) {
        // for a backward scan, nxt pointer is set to null
        // when the first log record in the first partition is set
        return RC(eEOF);
    }

    auto p = _storage->get_partition(ll.hi());
    W_DO(p->open_for_read());
    w_assert1(p);

    logrec_t* rp;
    lsn_t prev_lsn = lsn_t::null;
    DBGOUT3(<< "fetch @ lsn: " << ll);
    W_COERCE(p->read(rp, ll, forward ? NULL : &prev_lsn));
    w_assert0(rp->valid_header(ll));

    // handle skip log record
    if (rp->type() == logrec_t::t_skip)
    {
        p->release_read();
        if (forward) {
            DBGTHRD(<<"seeked to skip" << ll );
            DBGTHRD(<<"getting next partition.");
            ll = lsn_t(ll.hi() + 1, 0);

            p = _storage->get_partition(ll.hi());
            if(!p) {
                return RC(eEOF);
            }

            // re-read
            DBGOUT3(<< "fetch @ lsn: " << ll);
            W_DO(p->open_for_read());
            W_COERCE(p->read(rp, ll));
            w_assert0(rp->valid_header(ll));
        }
        else { // backward scan
            // just get previous log record using prev_lsn which was set inside
            // the first call to p->read()
            w_assert0(prev_lsn != lsn_t::null);
            ll = prev_lsn;
            DBGOUT3(<< "fetch @ lsn: " << ll);
            W_COERCE(p->read(rp, ll, &prev_lsn));
            w_assert0(rp->valid_header(ll));
        }
    }

    // set nxt pointer accordingly
    if (nxt) {
        if (!forward && prev_lsn == lsn_t::null) {
            if (ll == lsn_t(1,0)) {
                *nxt = lsn_t::null;
            }
            else {
                auto p = _storage->get_partition(ll.hi() - 1);
                *nxt = p ? lsn_t(p->num(), p->get_size()) : lsn_t::null;
            }
        }
        else {
            if (forward) {
                *nxt = ll;
                nxt->advance(rp->length());
            }
            else {
                *nxt = prev_lsn;
            }
        }
    }

    memcpy(buf, rp, rp->length());
    p->release_read();
    w_assert0(((logrec_t*) buf)->valid_header(ll));

    return RCOK;
}

void log_core::shutdown()
{
    // gnats 52:  RACE: We set _shutting_down and signal between the time
    // the daemon checks _shutting_down (false) and waits.
    //
    // So we need to notice if the daemon got the message.
    // It tells us it did by resetting the flag after noticing
    // that the flag is true.
    // There should be no interference with these two settings
    // of the flag since they happen strictly in that sequence.
    //
    _shutting_down = true;
    while (*&_shutting_down) {
        CRITICAL_SECTION(cs, _wait_flush_lock);
        // The only thread that should be waiting
        // on the _flush_cond is the log flush daemon.
        // Yet somehow we wedge here.
        DO_PTHREAD(pthread_cond_broadcast(&_flush_cond));
    }
    _flush_daemon->join();
    _flush_daemon_running = false;
    delete _flush_daemon;
    _flush_daemon=NULL;
}

/*********************************************************************
 *
 *  log_core::log_core(bufsize, reformat)
 *
 *  Open and scan logdir for master lsn and last log file.
 *  Truncate last incomplete log record (if there is any)
 *  from the last log file.
 *
 *********************************************************************/
log_core::log_core(const sm_options& options)
    :
      _start(0),
      _end(0),
      _waiting_for_flush(false),
      _shutting_down(false),
      _flush_daemon_running(false)
{
    

    _segsize = SEGMENT_SIZE;

    DO_PTHREAD(pthread_mutex_init(&_wait_flush_lock, NULL));
    DO_PTHREAD(pthread_cond_init(&_wait_cond, NULL));
    DO_PTHREAD(pthread_cond_init(&_flush_cond, NULL));

    uint32_t carray_slots = options.get_int_option("sm_carray_slots",
                        ConsolidationArray::DEFAULT_ACTIVE_SLOT_COUNT);
    _carray = new ConsolidationArray(carray_slots);

    /* Create thread o flush the log */
    _flush_daemon = new flush_daemon_thread_t(this);

    _buf = new char[_segsize];

    _storage = new log_storage(options);

    auto p = _storage->curr_partition();
    W_COERCE(p->open_for_read());
    _curr_lsn = _durable_lsn = _flush_lsn = lsn_t(p->num(), p->get_size(false));

    size_t prime_offset = 0;
    W_COERCE(p->prime_buffer(_buf, _durable_lsn, prime_offset));
    cerr << "Initialized curr_lsn to " << _curr_lsn << endl;

    _oldest_lsn_tracker = new PoorMansOldestLsnTracker(1 << 20);

    /* FRJ: the new code assumes that the buffer is always aligned
       with some buffer-sized multiple of the partition, so we need to
       return how far into the current segment we are.
        CS: moved this code from the _prime method
     */
    long offset = _durable_lsn.lo() % segsize();
    long base = _durable_lsn.lo() - offset;
    lsn_t start_lsn(_durable_lsn.hi(), base);

    // This should happend only in recovery/startup case.  So let's assert
    // that there is no log daemon running yet. If we ever fire this
    // assert, we'd better see why and it means we might have to protect
    // _cur_epoch and _start/_end with a critical section on _insert_lock.
    w_assert1(_flush_daemon_running == false);
    _buf_epoch = _cur_epoch = epoch(start_lsn, base, offset, offset);
    _end = _start = _durable_lsn.lo();

    // move the primed data where it belongs (watch out, it might overlap)
    memmove(_buf + offset - prime_offset, _buf, prime_offset);

    _ticker = NULL;
    if (options.get_bool_option("sm_ticker_enable", false)) {
        bool msec = options.get_bool_option("sm_ticker_msec", false);
        _ticker = new ticker_thread_t(msec);
    }

    // Load fetch buffers
    int fetchbuf_partitions = options.get_int_option("sm_log_fetch_buf_partitions", 0);
    if (fetchbuf_partitions > 0) {
        _fetch_buf_last = _durable_lsn.hi();
        _fetch_buf_first = _fetch_buf_last - fetchbuf_partitions + 1;
        _fetch_buf_end = _durable_lsn;
        _fetch_buf_begin = _durable_lsn;
    }
    else {
        _fetch_buf_last = 0;
        _fetch_buf_first = 0;
        _fetch_buf_end = lsn_t::null;
        _fetch_buf_begin = lsn_t::null;
    }
    _fetch_buf_loader = NULL;

    if (1) {
        cerr << "Log _start " << start_byte() << " end_byte() " << end_byte() << endl
            << "Log _curr_lsn " << _curr_lsn << " _durable_lsn " << _durable_lsn << endl;
        cerr << "Curr epoch  base_lsn " << _cur_epoch.base_lsn
            << "  base " << _cur_epoch.base
            << "  start " << _cur_epoch.start
            << "  end " << _cur_epoch.end << endl;
        cerr << "Old epoch  base_lsn " << _old_epoch.base_lsn
            << "  base " << _old_epoch.base
            << "  start " << _old_epoch.start
            << "  end " << _old_epoch.end << endl;
    }
}

rc_t log_core::init()
{
    // Consider this the beginning of log analysis so that
    // we can factor in the time it takes to load the fetch buffers
    sysevent::log(logrec_t::t_loganalysis_begin);
    if (_ticker) {
        _ticker->fork();
    }
    if (_fetch_buf_first > 0) {
        _fetch_buf_loader = new fetch_buffer_loader_t(this);
        _fetch_buf_loader->fork();
    }
    start_flush_daemon();

    return RCOK;
}

log_core::~log_core()
{
    if (_ticker) {
        _ticker->shutdown();
        _ticker->join();
        delete _ticker;
    }

    discard_fetch_buffers();

    delete _storage;
    delete _oldest_lsn_tracker;

    delete [] _buf;
    _buf = NULL;

    delete _carray;

    DO_PTHREAD(pthread_mutex_destroy(&_wait_flush_lock));
    DO_PTHREAD(pthread_cond_destroy(&_wait_cond));
    DO_PTHREAD(pthread_cond_destroy(&_flush_cond));
}

void log_core::_acquire_buffer_space(CArraySlot* info, long recsize)
{
    w_assert2(recsize > 0);


  /* Copy our data into the log buffer and update/create epochs.
   * Re: Racing flush daemon over
   * epochs, _start, _end, _curr_lsn, _durable_lsn :
   *
   * _start is set by  _prime at startup (no log flush daemon yet)
   *                   and flush_daemon_work, not by insert
   * _end is set by  _prime at startup (no log flush daemon yet)
   *                   and insert, not by log flush daemon
   * _old_epoch is  protected by _flush_lock
   * _cur_epoch is  protected by _flush_lock EXCEPT
   *                when insert does not wrap, it sets _cur_epoch.end,
   *                which is only read by log flush daemon
   *                The _end is only set after the memcopy is done,
   *                so this should be safe.
   * _curr_lsn is set by  insert (and at startup)
   * _durable_lsn is set by flush daemon
   *
   * NOTE: _end, _start, epochs updated in
   * _prime(), but that is called only for
   * opening a partition for append in startup/recovery case,
   * in which case there is no race to be had.
   *
   * It is also updated below.
   */

    /*
    * Make sure there's actually space available in the
    * log buffer,
    * accounting for the fact that flushes (by the daemon)
    * always work with full blocks. (they round down start of
    * flush to beginning of block and round up/pad end of flush to
    * end of block).
    *
    * If not, kick the flush daemon to make space.
    */
    // CS: TODO commented out waiting-for-space stuff
    //while(*&_waiting_for_space ||
    while(
            end_byte() - start_byte() + recsize > segsize() - 2* log_storage::BLOCK_SIZE)
    {
        _insert_lock.release(&info->me);
        {
            CRITICAL_SECTION(cs, _wait_flush_lock);
            while(end_byte() - start_byte() + recsize > segsize() - 2* log_storage::BLOCK_SIZE)
            {
                // CS: changed from waiting_for_space to waiting_for_flush
                _waiting_for_flush = true;
                // Use signal since the only thread that should be waiting
                // on the _flush_cond is the log flush daemon.
                DO_PTHREAD(pthread_cond_signal(&_flush_cond));
                DO_PTHREAD(pthread_cond_wait(&_wait_cond, &_wait_flush_lock));
            }
        }
        _insert_lock.acquire(&info->me);
    }
    // lfence because someone else might have entered and left during above release/acquire.
    lintel::atomic_thread_fence(lintel::memory_order_consume);
    // Having ics now should mean that even if another insert snuck in here,
    // we're ok since we recheck the condition. However, we *could* starve here.


  /* _curr_lsn, _cur_epoch.end, and end_byte() are all strongly related.
   *
   * _curr_lsn is the lsn of first byte past the tail of the log.
   *    Tail of the log is the last byte of the last record inserted (does not
   *    include the skip_log record).  Inserted records go to _curr_lsn,
   *    _curr_lsn moves with records inserted.
   *
   * _cur_epoch.end and end_byte() are convenience variables to avoid doing
   * expensive operations like modulus and division on the _curr_lsn
   * (lsn of next record to be inserted):
   *
   * _cur_epoch.end is the position of the current lsn relative to the
   *    segsize() log buffer.
   *    It is relative to the log buffer, and it wraps (modulo the
   *    segment size, which is the log buffer size). ("spill")
   *
   * end_byte()/_end is the non-wrapping version of _cur_epoch.end:
   *     at log init time it is set to a value in the range [0, segsize())
   *     and is  incremented by every log insert for the lifetime of
   *     the database.
   *     It is a byte-offset from the beginning of the log, considering that
   *     partitions are not "contiguous" in this sense.  Each partition has
   *     a lot of byte-offsets that aren't really in the log because
   *     we limit the size of a partition.
   *
   * _durable_lsn is the lsn of the first byte past the last
   *     log record written to the file (not counting the skip log record).
   *
   * _cur_epoch.start and start_byte() are convenience variables to avoid doing
   * expensive operations like modulus and division on the _durable_lsn
   *
   * _cur_epoch.start is the position of the durable lsn relative to the
   *     segsize() log buffer.   Because the _cur_epoch.end wraps, start
   *     could become > end and this would create a mess.  For this
   *     reason, when the log buffer wraps, we create a new
   *     epoch. The old epoch represents the entire unflushed
   *     portion of the old log buffer (including a portion of the
   *     presently-inserted log record) and the new epoch represents
   *     the next segment (logically, log buffer), containing
   *     the wrapped portion of the presently-inserted log record.
   *
   *     If, however, by starting a new segment to handle the wrap, we
   *     would exceed the partition size, we create a new epoch and
   *     new segment to hold the entire log record -- log records do not
   *     span partitions.   So we make the old epoch represent
   *     the unflushed portion of the old log buffer (not including any
   *     part of this record) and the new epoch represents the first segment
   *     in the new partition, and contains the entire presently-inserted
   *     log record.  We write the inserted log record at the beginning
   *     of the log buffer.
   *
   * start_byte()/_start is the non-wrapping version of _cur_epoch.start:
   *     At log init time it is set to 0
   *     and is bumped to match the durable lsn by every log flush.
   *     It is a byte-offset from the beginning of the log, considering that
   *     partitions are not "contiguous" in this sense.  Each partition has
   *     a lot of byte-offsets that aren't really in the log because
   *     we limit the size of a partition.
   *
   * start_byte() through end_byte() tell us the unflushed portion of the
   * log.
   */

  /* An epoch fits within a segment */
    w_assert2(_buf_epoch.end >= 0 && _buf_epoch.end <= segsize());

  /* end_byte() is the byte-offset-from-start-of-log-file
   * version of _cur_epoch.end */
    w_assert2(_buf_epoch.end % segsize() == end_byte() % segsize());

  /* _curr_lsn is the lsn of the next-to-be-inserted log record, i.e.,
   * the next byte of the log to be written
   */
  /* _cur_epoch.end is the offset into the log buffer of the _curr_lsn */
    w_assert2(_buf_epoch.end % segsize() == _curr_lsn.lo() % segsize());
  /* _curr_epoch.end should never be > segsize at this point;
   * that would indicate a wraparound is in progress when we entered this
   */
    w_assert2(end_byte() >= start_byte());

    // The following should be true since we waited on a condition
    // variable while
    // end_byte() - start_byte() + recsize > segsize() - 2*BLOCK_SIZE
    w_assert2(end_byte() - start_byte() <= segsize() - 2* log_storage::BLOCK_SIZE);


    long end = _buf_epoch.end;
    long old_end = _buf_epoch.base + end;
    long new_end = end + recsize;
    // set spillsize to the portion of the new record that
    // wraps around to the beginning of the log buffer(segment)
    long spillsize = new_end - segsize();
    lsn_t curr_lsn = _curr_lsn;
    lsn_t next_lsn = _buf_epoch.base_lsn + new_end;
    long new_base = -1;
    long start_pos = end;

    if(spillsize <= 0) {
        // update epoch for next log insert
        _buf_epoch.end = new_end;
    }
    // next_lsn is first byte after the tail of the log.
    else if(next_lsn.lo() <= _storage->get_partition_size()) {
        // wrap within a partition
        _buf_epoch.base_lsn += _segsize;
        _buf_epoch.base += _segsize;
        _buf_epoch.start = 0;
        _buf_epoch.end = new_end = spillsize;
    }
    else {
        curr_lsn = first_lsn(next_lsn.hi()+1);
        next_lsn = curr_lsn + recsize;
        new_base = _buf_epoch.base + _segsize;
        start_pos = 0;
        _buf_epoch = epoch(curr_lsn, new_base, 0, new_end=recsize);
    }

    // let the world know
    _curr_lsn = next_lsn;
    _end = _buf_epoch.base + new_end;

    _carray->join_expose(info);
    _insert_lock.release(&info->me);

    info->lsn = curr_lsn; // where will we end up on disk?
    info->old_end = old_end; // lets us serialize with our predecessor after memcpy
    info->start_pos = start_pos; // != old_end when partitions wrap
    info->pos = start_pos + recsize; // coordinates groups of threads sharing a log allocation
    info->new_end = new_end; // eventually assigned to _cur_epoch
    info->new_base = new_base; // positive if we started a new partition
    info->error = w_error_ok;
}

/**
 * Finish current log partition and start writing to a new one.
 */
rc_t log_core::truncate()
{
    // We want exclusive access to the log, so no CArray
    mcs_lock::qnode me;
    _insert_lock.acquire(&me);

    // create new empty epoch at the beginning of the new partition
    _curr_lsn = first_lsn(_buf_epoch.base_lsn.hi()+1);
    long new_base = _buf_epoch.base + _segsize;
    _buf_epoch = epoch(_curr_lsn, new_base, 0, 0);
    _end = new_base;

    // Update epochs so flush daemon can take over
    {
        CRITICAL_SECTION(cs, _flush_lock);
        // CS TODO: I don't know why this assertion should hold,
        // but here it fails sometimes.
        // w_assert3(_old_epoch.start == _old_epoch.end);
        _old_epoch = _cur_epoch;
        _cur_epoch = epoch(_curr_lsn, new_base, 0, 0);
    }

    _insert_lock.release(&me);

    return RCOK;
}

lsn_t log_core::_copy_to_buffer(logrec_t &rec, long pos, long recsize, CArraySlot* info)
{
    /*
      do the memcpy (or two)
    */
    lsn_t rlsn = info->lsn + pos;
    rec.set_lsn_ck(rlsn);

    _copy_raw(info, pos, (char const*) &rec, recsize);

    return rlsn;
}

bool log_core::_update_epochs(CArraySlot* info) {
    w_assert1(info->vthis()->count == ConsolidationArray::SLOT_FINISHED);
    // Wait for our predecessor to catch up if we're ahead.
    // Even though the end pointer we're checking wraps regularly, we
    // already have to limit each address in the buffer to one active
    // writer or data corruption will result.
    if (CARRAY_RELEASE_DELEGATION) {
        if(_carray->wait_for_expose(info)) {
            return true; // we delegated!
        }
    } else {
        // If delegated-buffer-release is off, we simply spin until predecessors complete.
        lintel::atomic_thread_fence(lintel::memory_order_seq_cst);
        while(*&_cur_epoch.vthis()->end + *&_cur_epoch.vthis()->base != info->old_end);
    }

    // now update the epoch(s)
    while (info != NULL) {
        w_assert1(*&_cur_epoch.vthis()->end + *&_cur_epoch.vthis()->base == info->old_end);
        if(info->new_base > 0) {
            // new partition! update epochs to reflect this

            // I just wrote part of the log record to the beginning of the
            // log buffer. How do I know that it didn't interfere with what
            // the flush daemon is writing? Because at the beginning of
            // this method, I waited until the log flush daemon ensured that
            // I had room to insert this entire record (with a fudge factor
            // of 2*BLOCK_SIZE)

            // update epochs
            CRITICAL_SECTION(cs, _flush_lock);
            w_assert3(_old_epoch.start == _old_epoch.end);
            _old_epoch = _cur_epoch;
            _cur_epoch = epoch(info->lsn, info->new_base, 0, info->new_end);
        }
        else if(info->pos > _segsize) {
            // wrapped buffer! update epochs
            CRITICAL_SECTION(cs, _flush_lock);
            w_assert3(_old_epoch.start == _old_epoch.end);
            _old_epoch = epoch(_cur_epoch.base_lsn, _cur_epoch.base,
                        _cur_epoch.start, segsize());
            _cur_epoch.base_lsn += segsize();
            _cur_epoch.base += segsize();
            _cur_epoch.start = 0;
            _cur_epoch.end = info->new_end;
        }
        else {
            // normal update -- no need for a lock if we just increment its end
            w_assert1(_cur_epoch.start < info->new_end);
            _cur_epoch.end = info->new_end;
        }

        // we might have to also release delegated buffer(s).
        info = _carray->grab_delegated_expose(info);
    }

    return false;
}

rc_t log_core::_join_carray(CArraySlot*& info, long& pos, int32_t size)
{
    /* Copy our data into the buffer and update/create epochs. Note
       that, while we may race the flush daemon to update the epoch
       record, it will not touch the buffer until after we succeed so
       there is no race with memcpy(). If we do lose an epoch update
       race, it is only because the flush changed old_epoch.begin to
       equal old_epoch.end. The mutex ensures we don't race with any
       other inserts.
    */

    // consolidate
    carray_status_t old_count;
    info = _carray->join_slot(size, old_count);

    pos = ConsolidationArray::extract_carray_log_size(old_count);
    if(old_count == ConsolidationArray::SLOT_AVAILABLE) {
        /* First to arrive. Acquire the lock on behalf of the whole
        * group, claim the first 'size' bytes, then make the rest
        * visible to waiting threads.
        */
        _insert_lock.acquire(&info->me);

        w_assert1(info->vthis()->count > ConsolidationArray::SLOT_AVAILABLE);
        w_assert1(pos == 0);

        // swap out this slot and mark it busy
        _carray->replace_active_slot(info);

        // negate the count to signal waiting threads and mark the slot busy
        old_count = lintel::unsafe::atomic_exchange<carray_status_t>(
            &info->count, ConsolidationArray::SLOT_PENDING);
        long combined_size = ConsolidationArray::extract_carray_log_size(old_count);

        // grab space for everyone in one go (releases the lock)
        _acquire_buffer_space(info, combined_size);

        // now let everyone else see it
        lintel::atomic_thread_fence(lintel::memory_order_seq_cst);
        info->count = ConsolidationArray::SLOT_FINISHED - combined_size;
    }
    else {
        // Not first. Wait for the owner to tell us what's going on.
        w_assert1(old_count > ConsolidationArray::SLOT_AVAILABLE);
        _carray->wait_for_leader(info);
    }
    return RCOK;
}

rc_t log_core::_leave_carray(CArraySlot* info, int32_t size)
{
    // last one to leave cleans up
    carray_status_t end_count = lintel::unsafe::atomic_fetch_add<carray_status_t>(
        &info->count, size);
    end_count += size; // NOTE lintel::unsafe::atomic_fetch_add returns the value before the
    // addition. So, we need to add it here again. atomic_add_fetch desired..
    w_assert3(end_count <= ConsolidationArray::SLOT_FINISHED);
    if(end_count == ConsolidationArray::SLOT_FINISHED) {
        if(!info->error) {
            _update_epochs(info);
        }
    }

    if(info->error) {
        return RC(info->error);
    }

    return RCOK;
}

rc_t log_core::insert(logrec_t &rec, lsn_t* rlsn)
{
    w_assert1(rec.length() <= sizeof(logrec_t));
    int32_t size = rec.length();

    CArraySlot* info = NULL;
    long pos = 0;
    W_DO(_join_carray(info, pos, size));
    w_assert1(info);

    // insert my value
    lsn_t rec_lsn;
    if(!info->error) {
        rec_lsn = _copy_to_buffer(rec, pos, size, info);
    }

    W_DO(_leave_carray(info, size));

    if(rlsn) {
        *rlsn = rec_lsn;
    }
    DBGOUT3(<< " insert @ lsn: " << rec_lsn << " type " << rec.type() << " length " << rec.length() );

    ADD_TSTAT(log_bytes_generated,size);
    return RCOK;
}

void log_core::_copy_raw(CArraySlot* info, long& pos, const char* data,
        size_t size)
{
    // are we the ones that actually wrap? (do this *after* computing the lsn!)
    pos += info->start_pos;
    if(pos >= _segsize)
        pos -= _segsize;

    long spillsize = pos + size - _segsize;
    if(spillsize <= 0) {
        // normal insert
        memcpy(_buf+pos, data, size);
    }
    else {
        // spillsize > 0 so we are wrapping.
        // The wrap is within a partition.
        // next_lsn is still valid but not new_end
        //
        // Old epoch becomes valid for the flush daemon to
        // flush and "close".  It contains the first part of
        // this log record that we're trying to insert.
        // New epoch holds the rest of the log record that we're trying
        // to insert.
        //
        // spillsize is the portion that wraps around
        // partsize is the portion that doesn't wrap.
        long partsize = size - spillsize;

        // Copy log record to buffer
        // memcpy : areas do not overlap
        memcpy(_buf+pos, data, partsize);
        memcpy(_buf, data+partsize, spillsize);
    }
}

/*
 * Inserts an arbitrary block of memory (a bulk of log records from plog) into
 * the log buffer, returning the LSN of the first byte in rlsn. This is used
 * by the atomic commit protocol (see plog_xct_t::_commit) (Caetano).
 */
/*rc_t log_core::insert_bulk(char* data, size_t size, lsn_t*& rlsn)
{
    CArraySlot* info = NULL;
    long pos = 0;
    W_DO(_join_carray(info, pos, size));
    w_assert1(info);

    // copy to buffer (similar logic of _copy_to_buffer, but for bulk use)
    if(!info->error) {
        *rlsn = info->lsn + pos;


    W_DO(_leave_carray(info, size));

    ADD_TSTAT(log_bytes_generated,size);
    return RCOK;
}
*/


// Return when we know that the given lsn is durable. Wait for the
// log flush daemon to ensure that it's durable.
rc_t log_core::flush(const lsn_t &to_lsn, bool block, bool signal, bool *ret_flushed)
{
    DBGOUT3(<< " flush @ to_lsn: " << to_lsn);

    w_assert1(signal || !block); // signal=false can be used only when block=false
    ASSERT_FITS_IN_POINTER(lsn_t);
    // else our reads to _durable_lsn would be unsafe

    // don't try to flush past end of log -- we might wait forever...
    lsn_t lsn = std::min(to_lsn, (*&_curr_lsn)+ -1);

    // already durable?
    if(lsn >= *&_durable_lsn) {
        if (!block) {
            *&_waiting_for_flush = true;
            if (signal) {
                DO_PTHREAD(pthread_cond_signal(&_flush_cond));
            }
            if (ret_flushed) *ret_flushed = false; // not yet flushed
        }  else {
            CRITICAL_SECTION(cs, _wait_flush_lock);
            while(lsn >= *&_durable_lsn) {
                *&_waiting_for_flush = true;
                // Use signal since the only thread that should be waiting
                // on the _flush_cond is the log flush daemon.
                DO_PTHREAD(pthread_cond_signal(&_flush_cond));
                DO_PTHREAD(pthread_cond_wait(&_wait_cond, &_wait_flush_lock));
            }
            if (ret_flushed) *ret_flushed = true;// now flushed!
        }
    } else {
        INC_TSTAT(log_dup_sync_cnt);
        if (ret_flushed) *ret_flushed = true; // already flushed
    }
    return RCOK;
}

/**\brief Log-flush daemon driver.
 * \details
 * This method handles the wait/block of the daemon thread,
 * and when awake, calls its main-work method, flush_daemon_work.
 */
void log_core::flush_daemon()
{
    /* Algorithm: attempt to flush non-durable portion of the buffer.
     * If we empty out the buffer, block until either enough
       bytes get written or a thread specifically requests a flush.
     */
    lsn_t last_completed_flush_lsn;
    bool success = false;
    while(1) {

        // wait for a kick. Kicks come at regular intervals from
        // inserts, but also at arbitrary times when threads request a
        // flush.
        {
            CRITICAL_SECTION(cs, _wait_flush_lock);
            // CS: commented out check for waiting_for_space -- don't know why it was here?
            //if(success && (*&_waiting_for_space || *&_waiting_for_flush)) {
            if(success && *&_waiting_for_flush) {
                //_waiting_for_flush = _waiting_for_space = false;
                _waiting_for_flush = false;
                DO_PTHREAD(pthread_cond_broadcast(&_wait_cond));
                // wake up anyone waiting on log flush
            }
            if(_shutting_down) {
                _shutting_down = false;
                break;
            }

            // NOTE: right now the thread waiting for a flush has woken up or will woke up, but...
            // this thread, as long as success is true (it just flushed something in the previous
            // flush_daemon_work), will keep calling flush_daemon_work until there is nothing to flush....
            // this happens in the background

            // sleep. We don't care if we get a spurious wakeup
            //if(!success && !*&_waiting_for_space && !*&_waiting_for_flush) {
            if(!success && !*&_waiting_for_flush) {
                // Use signal since the only thread that should be waiting
                // on the _flush_cond is the log flush daemon.
                DO_PTHREAD(pthread_cond_wait(&_flush_cond, &_wait_flush_lock));
            }
        }

        // flush all records later than last_completed_flush_lsn
        // and return the resulting last durable lsn
        lsn_t lsn = flush_daemon_work(last_completed_flush_lsn);

        // success=true if we wrote anything
        success = (lsn != last_completed_flush_lsn);
        last_completed_flush_lsn = lsn;
    }

    // make sure the buffer is completely empty before leaving...
    for(lsn_t lsn;
        (lsn=flush_daemon_work(last_completed_flush_lsn)) !=
                last_completed_flush_lsn;
        last_completed_flush_lsn=lsn) ;
}

/**\brief Flush unflushed-portion of log buffer.
 * @param[in] old_mark Durable lsn from last flush. Flush records later than this.
 * \details
 * This is the guts of the log daemon.
 *
 * Flush the log buffer of any log records later than \a old_mark. The
 * argument indicates what is already durable and these log records must
 * not be duplicated on the disk.
 *
 * Called by the log flush daemon.
 * Protection from duplicate flushing is handled by the fact that we have
 * only one log flush daemon.
 * \return Latest durable lsn resulting from this flush
 *
 */
lsn_t log_core::flush_daemon_work(lsn_t old_mark)
{
    lsn_t base_lsn_before, base_lsn_after;
    long base, start1, end1, start2, end2;
    {
        CRITICAL_SECTION(cs, _flush_lock);
        base_lsn_before = _old_epoch.base_lsn;
        base_lsn_after = _cur_epoch.base_lsn;
        base = _cur_epoch.base;

        // The old_epoch is valid (needs flushing) iff its end > start.
        // The old_epoch is valid id two cases, both when
        // insert wrapped thelog buffer
        // 1) by doing so reached the end of the partition,
        //     In this case, the old epoch might not be an entire
        //     even segment size
        // 2) still room in the partition
        //     In this case, the old epoch is exactly 1 segment in size.

        if(_old_epoch.start == _old_epoch.end) {
            // no wrap -- flush only the new
            w_assert1(_cur_epoch.end >= _cur_epoch.start);
            start2 = _cur_epoch.start;
            end2 = _cur_epoch.end;
            w_assert1(end2 >= start2);
            // false alarm?
            if(start2 == end2) {
                return old_mark;
            }
            _cur_epoch.start = end2;

            start1 = start2; // fake start1 so the start_lsn calc below works
            end1 = start2;

            base_lsn_before = base_lsn_after;
        }
        else if(base_lsn_before.file() == base_lsn_after.file()) {
            // wrapped within partition -- flush both
            start2 = _cur_epoch.start;
            // race here with insert setting _curr_epoch.end, but
            // it won't matter. Since insert already did the memcpy,
            // we are safe and can flush the entire amount.
            end2 = _cur_epoch.end;
            _cur_epoch.start = end2;

            start1 = _old_epoch.start;
            end1 = _old_epoch.end;
            _old_epoch.start = end1;

            w_assert1(base_lsn_before + segsize() == base_lsn_after);
        }
        else {
            // new partition -- flushing only the old since the
            // two epochs target different files. Let the next
            // flush handle the new epoch.
            start2 = 0;
            end2 = 0; // don't fake end2 because end_lsn needs to see '0'

            start1 = _old_epoch.start;
            end1 = _old_epoch.end;

            // Mark the old epoch has no longer valid.
            _old_epoch.start = end1;

            w_assert1(base_lsn_before.file()+1 == base_lsn_after.file());
        }
    } // end critical section

    lsn_t start_lsn = base_lsn_before + start1;
    lsn_t end_lsn   = base_lsn_after + end2;
    long  new_start = base + end2;
    {
        // Avoid interference with compensations.
        CRITICAL_SECTION(cs, _comp_lock);
        _flush_lsn = end_lsn;
    }

    w_assert1(end1 >= start1);
    w_assert1(end2 >= start2);
    w_assert1(end_lsn == first_lsn(start_lsn.hi()+1)
          || end_lsn.lo() - start_lsn.lo() == (end1-start1) + (end2-start2));

    // start_lsn.file() determines partition # and whether code
    // will open a new partition into which to flush.
    // That, in turn, is determined by whether the _old_epoch.base_lsn.file()
    // matches the _cur_epoch.base_lsn.file()
    // CS: This code used to be on the method _flushX
    auto p = _storage->get_partition_for_flush(start_lsn, start1, end1,
            start2, end2);

    // Flush the log buffer
    W_COERCE(p->flush(start_lsn, _buf, start1, end1, start2, end2));

    long written = (end2 - start2) + (end1 - start1);
    //cout << "Flushed " << written << " bytes" << endl;
    p->set_size(start_lsn.lo()+written);


    _durable_lsn = end_lsn;
    _start = new_start;

    return end_lsn;
}

// Find the log record at orig_lsn and turn it into a compensation
// back to undo_lsn
rc_t log_core::compensate(const lsn_t& orig_lsn, const lsn_t& undo_lsn)
{
    // somewhere in the calling code, we didn't actually log anything.
    // so this would be a compensate to myself.  i.e. a no-op
    if(orig_lsn == undo_lsn)
        return RCOK;

    // FRJ: this assertion wasn't there originally, but I don't see
    // how the situation could possibly be correct
    w_assert1(orig_lsn <= _curr_lsn);

    // no need to grab a mutex if it's too late
    if(orig_lsn < _flush_lsn)
    {
        DBGOUT3( << "log_core::compensate - orig_lsn: " << orig_lsn
                 << ", flush_lsn: " << _flush_lsn << ", undo_lsn: " << undo_lsn);
        return RC(eBADCOMPENSATION);
    }

    CRITICAL_SECTION(cs, _comp_lock);
    // check again; did we just miss it?
    lsn_t flsn = _flush_lsn;
    if(orig_lsn < flsn)
        return RC(eBADCOMPENSATION);

    /* where does it live? the buffer is always aligned with a
       buffer-sized chunk of the partition, so all we need to do is
       take a modulus of the lsn to get its buffer position. It may be
       wrapped but we know it's valid because we're less than a buffer
       size from the current _flush_lsn -- nothing newer can be
       overwritten until we release the mutex.
     */
    long pos = orig_lsn.lo() % segsize();
    if(pos >= segsize() - logrec_t::hdr_non_ssx_sz)
        return RC(eBADCOMPENSATION); // split record. forget it.

    // aligned?
    w_assert1((pos & 0x7) == 0);

    // grab the record and make sure it's valid
    logrec_t* s = (logrec_t*) &_buf[pos];

    // valid length?
    w_assert1((s->length() & 0x7) == 0);

    // split after the log header? don't mess with it
    if(pos + long(s->length()) > segsize())
        return RC(eBADCOMPENSATION);

    lsn_t lsn_ck = s->get_lsn_ck();
    if(lsn_ck != orig_lsn) {
        // this is a pretty rare occurrence, and I haven't been able
        // to figure out whether it's actually a bug
        cerr << endl << "lsn_ck = "<<lsn_ck.hi()<<"."<<lsn_ck.lo()<<", orig_lsn = "<<orig_lsn.hi()<<"."<<orig_lsn.lo()<<endl
            << __LINE__ << " " __FILE__ << " "
            << "log rec is  " << *s << endl;
                return RC(eBADCOMPENSATION);
    }
    if (!s->is_single_sys_xct()) {
        w_assert1(s->xid_prev() == lsn_t::null || s->xid_prev() >= undo_lsn);

        if(s->is_undoable_clr())
            return RC(eBADCOMPENSATION);

        // success!
        DBGTHRD(<<"COMPENSATING LOG RECORD " << undo_lsn << " : " << *s);
        s->set_clr(undo_lsn);
    }
    return RCOK;
}

rc_t log_core::load_fetch_buffers()
{
    _fetch_buffers.resize(_fetch_buf_last - _fetch_buf_first + 1, NULL);

    for (size_t p = _fetch_buf_last; p >= _fetch_buf_first; p--) {
        int fd;

        string fname = _storage->make_log_name(p);
        int flags = smthread_t::OPEN_RDONLY;

        // get file size and whether it exists
        struct stat file_info;
        int status = stat(fname.c_str(), &file_info);
        if (status < 0) {
            continue;
        }

        // Allocate buffer space and open file
        char* buf = new char[file_info.st_size];
        _fetch_buffers[p - _fetch_buf_first] = buf;
        W_DO(me()->open(fname.c_str(), flags, 0744, fd));

        // Main loop that loads chunks of 32MB in reverse sequential order
        size_t chunk = 32 * 1024 * 1024;
        long offset = file_info.st_size - chunk;
        while (true) {
            size_t read_size = offset >= 0 ? chunk : chunk + offset;
            if (offset < 0) { offset = 0; }

            W_DO(me()->pread(fd, buf + offset, read_size, offset));

            // CS TODO: use std::atomic
            _fetch_buf_begin = lsn_t(p, offset);
            lintel::atomic_thread_fence(lintel::memory_order_release);

            if (offset == 0) { break; }
            offset -= read_size;
        }

        W_DO(me()->close(fd));

        // size_t pos = 0;
        // while (pos < file_info.st_size) {
        //     logrec_t* lr = (logrec_t*) (_fetch_buffers[p - _fetch_buf_first] + pos);
        //     w_assert0(lr->valid_header(lsn_t(p, pos)));
        //     ERROUT(<< *lr);
        //     pos += lr->length();
        // }
    }
    return RCOK;
}

void log_core::discard_fetch_buffers()
{
    if (_fetch_buf_loader) {
        _fetch_buf_loader->join();
        delete _fetch_buf_loader;
    }

    for (size_t p = _fetch_buf_first; p > 0 && p <= _fetch_buf_last; p++) {
        size_t i = p - _fetch_buf_first;
        if (_fetch_buffers[i]) {
            delete[] _fetch_buffers[i];
        }
    }

    _fetch_buffers.clear();
    _fetch_buf_first = 0;
    _fetch_buf_last = 0;
    _fetch_buf_begin = lsn_t::null;
    _fetch_buf_end = lsn_t::null;
}

/*********************************************************************
 *
 *  log_i::xct_next(lsn, r)
 *
 *  Read the next record into r and return its lsn in lsn.
 *  Return false if EOF reached. true otherwise.
 *
 *********************************************************************/
bool log_i::xct_next(lsn_t& lsn, logrec_t& r)
{
    // Initially (before the first xct_next call,
    // 'cursor' is set to the starting point of the scan
    // After each xct_next call,
    // 'cursor' is set to the lsn of the next log record if forward scan
    // or the lsn of the fetched log record if backward scan
    // log.fetch() returns eEOF when it reaches the end of the scan

    bool eof = (cursor == lsn_t::null);

    if (! eof) {
        lsn = cursor;
        rc_t rc = log.fetch(lsn, &r, &cursor, forward_scan);  // Either forward or backward scan

        if (rc.is_error())  {
            last_rc = RC_AUGMENT(rc);
            RC_APPEND_MSG(last_rc, << "trying to fetch lsn " << cursor);

            if (last_rc.err_num() == eEOF)
                eof = true;
            else  {
                cerr << "Fatal error : " << last_rc << endl;
            }
        }
    }

    return ! eof;
}
