/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#include "bf_hashtable.h"
#include "bf_tree_cb.h"
#include "bf_tree_cleaner.h"
#include "page_cleaner_decoupled.h"
#include "bf_tree.h"

#include "smthread.h"
#include "generic_page.h"
#include <string.h>
#include "w_findprime.h"
#include <stdlib.h>

#include "sm_base.h"
#include "sm.h"
#include "vol.h"
#include "alloc_cache.h"

#include <boost/static_assert.hpp>
#include <ostream>
#include <limits>
#include <algorithm>

#include "sm_options.h"
#include "latch.h"
#include "btree_page_h.h"
#include "log_core.h"
#include "xct.h"
#include <logfunc_gen.h>

#include "restart.h"

// Template definitions
#include "bf_hashtable.cpp"

///////////////////////////////////   Initialization and Release BEGIN ///////////////////////////////////

#ifdef PAUSE_SWIZZLING_ON
bool bf_tree_m::_bf_pause_swizzling = true;
uint64_t bf_tree_m::_bf_swizzle_ex = 0;
uint64_t bf_tree_m::_bf_swizzle_ex_fails = 0;
#endif // PAUSE_SWIZZLING_ON

// xum: hot page file
ofstream hp_file ("hot_pages.dump", ios::binary);

bf_tree_m::bf_tree_m(const sm_options& options)
{

    // sm_bufboolsize given in MB -- default 8GB
    long bufpoolsize = options.get_int_option("sm_bufpoolsize", 8192) * 1024 * 1024;
    uint32_t  nbufpages = (bufpoolsize - 1) / smlevel_0::page_sz + 1;
    if (nbufpages < 10)  {
        cerr << "ERROR: buffer size ("
             << bufpoolsize
             << "-KB) is too small" << endl;
        cerr << "       at least " << 32 * smlevel_0::page_sz / 1024
             << "-KB is needed" << endl;
        W_FATAL(eCRASH);
    }

    bool bufferpool_swizzle =
        options.get_bool_option("sm_bufferpool_swizzle", false);
    // clock or random
    std::string replacement_policy =
        options.get_string_option("sm_bufferpool_replacement_policy", "clock");

    ::memset (this, 0, sizeof(bf_tree_m));

    _block_cnt = nbufpages;
    _enable_swizzling = bufferpool_swizzle;
    // if (strcmp(replacement_policy.c_str(), "clock") == 0) {
    //     _replacement_policy = POLICY_CLOCK;
    // } else if (strcmp(replacement_policy.c_str(), "clock+priority") == 0) {
    //     _replacement_policy = POLICY_CLOCK_PRIORITY;
    // } else if (strcmp(replacement_policy.c_str(), "random") == 0) {
    //     _replacement_policy = POLICY_RANDOM;
    // }

#ifdef SIMULATE_NO_SWIZZLING
    _enable_swizzling = false;
    bufferpool_swizzle = false;
    DBGOUT0 (<< "THIS MESSAGE MUST NOT APPEAR unless you intended."
            << " Completely turned off swizzling in bufferpool.");
#endif // SIMULATE_NO_SWIZZLING

    DBGOUT1 (<< "constructing bufferpool with " << nbufpages << " blocks of "
            << SM_PAGESIZE << "-bytes pages... enable_swizzling=" <<
            _enable_swizzling);

    // use posix_memalign to allow unbuffered disk I/O
    void *buf = NULL;
    if (::posix_memalign(&buf, SM_PAGESIZE, SM_PAGESIZE * ((uint64_t)
                    nbufpages)) != 0)
    {
        ERROUT (<< "failed to reserve " << nbufpages
                << " blocks of " << SM_PAGESIZE << "-bytes pages. ");
        W_FATAL(eOUTOFMEMORY);
    }
    _buffer = reinterpret_cast<generic_page*>(buf);

    // the index 0 is never used. to make sure no one can successfully use it,
    // fill the block-0 with garbages
    ::memset (&_buffer[0], 0x27, sizeof(generic_page));

#ifdef BP_ALTERNATE_CB_LATCH
    // this allocation scheme is sensible only for control block and latch sizes of 64B (cacheline size)
    BOOST_STATIC_ASSERT(sizeof(bf_tree_cb_t) == 64);
    BOOST_STATIC_ASSERT(sizeof(latch_t) == 64);
    // allocate one more pair of <control block, latch> as we want to align the table at an odd
    // multiple of cacheline (64B)
    size_t total_size = (sizeof(bf_tree_cb_t) + sizeof(latch_t))
        * (((uint64_t) nbufpages) + 1LLU);
    if (::posix_memalign(&buf, sizeof(bf_tree_cb_t) + sizeof(latch_t),
                total_size) != 0)
    {
        ERROUT (<< "failed to reserve " << nbufpages
                << " blocks of " << sizeof(bf_tree_cb_t) << "-bytes blocks.");
        W_FATAL(eOUTOFMEMORY);
    }
    ::memset (buf, 0, (sizeof(bf_tree_cb_t) + sizeof(latch_t)) * (((uint64_t)
                    nbufpages) + 1LLU));
    _control_blocks = reinterpret_cast<bf_tree_cb_t*>(reinterpret_cast<char
            *>(buf) + sizeof(bf_tree_cb_t));
    w_assert0(_control_blocks != NULL);
    for (bf_idx i = 0; i < nbufpages; i++) {
        BOOST_STATIC_ASSERT(sizeof(bf_tree_cb_t) < SCHAR_MAX);
        if (i & 0x1) { /* odd */
            get_cb(i)._latch_offset = -static_cast<int8_t>(sizeof(bf_tree_cb_t)); // place the latch before the control block
        } else { /* even */
            get_cb(i)._latch_offset = sizeof(bf_tree_cb_t); // place the latch after the control block
        }
    }
#else
    if (::posix_memalign(&buf, sizeof(bf_tree_cb_t),
                sizeof(bf_tree_cb_t) * ((uint64_t) nbufpages)) != 0)
    {
        ERROUT (<< "failed to reserve " << nbufpages
                << " blocks of " << sizeof(bf_tree_cb_t) << "-bytes blocks. ");
        W_FATAL(eOUTOFMEMORY);
    }
    _control_blocks = reinterpret_cast<bf_tree_cb_t*>(buf);
    w_assert0(_control_blocks != NULL);
    ::memset (_control_blocks, 0, sizeof(bf_tree_cb_t) * nbufpages);
#endif

    // initially, all blocks are free
    _freelist = new bf_idx[nbufpages];
    w_assert0(_freelist != NULL);
    _freelist[0] = 1; // [0] is a special entry. it's the list head
    for (bf_idx i = 1; i < nbufpages - 1; ++i) {
        _freelist[i] = i + 1;
    }
    _freelist[nbufpages - 1] = 0;
    _freelist_len = nbufpages - 1; // -1 because [0] isn't a valid block

    //initialize hashtable
    int buckets = w_findprime(1024 + (nbufpages / 4)); // maximum load factor is 25%. this is lower than original shore-mt because we have swizzling
    _hashtable = new bf_hashtable<bf_idx_pair>(buckets);
    w_assert0(_hashtable != NULL);

    _swizzled_page_count_approximate = 0;

    _eviction_current_frame = 0;
    DO_PTHREAD(pthread_mutex_init(&_eviction_lock, NULL));

    _cleaner_decoupled = options.get_bool_option("sm_cleaner_decoupled", false);

    
}

void bf_tree_m::shutdown()
{
    if (_cleaner) {
        _cleaner->shutdown();
        delete _cleaner;
    }
}

bf_tree_m::~bf_tree_m()
{
    //xum
    hp_file.close();
    if (_control_blocks != NULL) {
#ifdef BP_ALTERNATE_CB_LATCH
        char* buf = reinterpret_cast<char*>(_control_blocks) - sizeof(bf_tree_cb_t);
#else
        char* buf = reinterpret_cast<char*>(_control_blocks);
#endif
        // note we use free(), not delete[], which corresponds to posix_memalign
        ::free (buf);
        _control_blocks = NULL;
    }
    if (_freelist != NULL) {
        delete[] _freelist;
        _freelist = NULL;
    }
    if (_hashtable != NULL) {
        delete _hashtable;
        _hashtable = NULL;
    }
    if (_buffer != NULL) {
        void *buf = reinterpret_cast<void*>(_buffer);
        // note we use free(), not delete[], which corresponds to posix_memalign
        ::free (buf);
        _buffer = NULL;
    }

    DO_PTHREAD(pthread_mutex_destroy(&_eviction_lock));

}

page_cleaner_base* bf_tree_m::get_cleaner()
{
    if (!ss_m::vol || !ss_m::vol->caches_ready()) {
        // No volume manager initialized -- no point in starting cleaner
        return nullptr;
    }

    if (!_cleaner) {
        if(_cleaner_decoupled) {
            w_assert0(smlevel_0::logArchiver);
            _cleaner = new page_cleaner_decoupled(this,
                    ss_m::get_options());
        }
        else{
            _cleaner = new bf_tree_cleaner (this, ss_m::get_options());
        }
        _cleaner->fork();
    }

    return _cleaner;
}

///////////////////////////////////   Initialization and Release END ///////////////////////////////////

bf_idx bf_tree_m::lookup(PageID pid) const
{
    bf_idx idx = 0;
    bf_idx_pair p;
    if (_hashtable->lookup(pid, p)) {
        idx = p.first;
    }
    return idx;
}

///////////////////////////////////   Page fix/unfix BEGIN         ///////////////////////////////////
// NOTE most of the page fix/unfix functions are in bf_tree_inline.h.
// These functions are here are because called less frequently.


void bf_tree_m::associate_page(generic_page*&_pp, bf_idx idx, PageID page_updated)
{
    // Special function for REDO phase of the system Recovery process
    // The physical page is loaded in buffer pool, idx is known but we
    // need to associate it with fixable_page data structure
    // Swizzling must be off

    w_assert1(!smlevel_0::bf->is_swizzling_enabled());
    w_assert1 (_is_active_idx(idx));
    _pp = &(smlevel_0::bf->_buffer[idx]);

    // Store lptid_t (vol and store IDs and page number) into the data buffer in the page
    _buffer[idx].pid = page_updated;

    return;
}

w_rc_t bf_tree_m::_fix_nonswizzled(generic_page* parent, generic_page*& page,
                                   PageID shpid, latch_mode_t mode,
                                   bool conditional, bool virgin_page,
                                   lsn_t emlsn)
{
    w_assert1((shpid & SWIZZLED_PID_BIT) == 0);

    // note that the hashtable is separated from this bufferpool.
    // we need to make sure the returned block is still there, and retry otherwise.
#if W_DEBUG_LEVEL>0
    int retry_count = 0;
#endif
    while (true)
    {
        const uint32_t ONE_MICROSEC = 10000;
#if W_DEBUG_LEVEL>0
        if (++retry_count % 10000 == 0) {
            DBGOUT1(<<"keep trying to fix.. " << shpid << ". current retry count=" << retry_count);
        }
#endif
        bf_idx_pair p;
        bf_idx idx = 0;
        if (_hashtable->lookup(shpid, p)) {
            idx = p.first;
            if (parent && p.second != parent - _buffer) {
                // need to fix update parent pointer
                p.second = parent - _buffer;
                _hashtable->update(shpid, p);
            }
        }

        if (idx == 0)
        {
            // 1. page is not registered in hash table therefore it is not an in_doubt page either
            // 2. page is registered in hash table already but force loading, meaning it is an
            //    in_doubt page and the actual page has not been loaded into buffer pool yet

            // STEP 1) Grab a free frame to read into
            W_DO(_grab_free_block(idx));
            w_assert1(idx != 0);
            bf_tree_cb_t &cb = get_cb(idx);

            // STEP 2) Acquire EX latch, so that only one thread attempts read
            w_rc_t check_rc = cb.latch().latch_acquire(LATCH_EX,
                    sthread_t::WAIT_IMMEDIATE);
            if (check_rc.is_error())
            {
                _add_free_block(idx);
                ::usleep(ONE_MICROSEC);
                continue;
            }

            // STEP 3) register the page on the hashtable, so that only one
            // thread reads it (it may be latched by other concurrent fix)
            bf_idx parent_idx = parent ? parent - _buffer : 0;
            bool registered = _hashtable->insert_if_not_exists(shpid,
                    bf_idx_pair(idx, parent_idx));
            if (!registered) {
                cb.clear_except_latch();
                cb.latch().latch_release();
                _add_free_block(idx);
                continue;
            }

            // STEP 4) Read page from disk
            page = &_buffer[idx];

            if (!virgin_page) {
                INC_TSTAT(bf_fix_nonroot_miss_count);

                if (parent && emlsn.is_null()) {
                    // Get emlsn from parent
                    general_recordid_t recordid = find_page_id_slot(parent, shpid);
                    btree_page_h parent_h;
                    parent_h.fix_nonbufferpool_page(parent);
                    emlsn = parent_h.get_emlsn_general(recordid);
                }

                w_rc_t read_rc = smlevel_0::vol->read_page_verify(shpid, page, emlsn);
                if (read_rc.is_error())
                {
                    _hashtable->remove(shpid);
                    cb.latch().latch_release();
                    _add_free_block(idx);
                    return read_rc;
                }
            }

            // STEP 5) initialize control block
            w_assert1(cb.latch().held_by_me());
            w_assert1(cb.latch().mode() == LATCH_EX);

            cb.clear_except_latch();
            cb._pin_cnt = 0;
            cb._pid = shpid;
            cb._used = true;
            cb._ref_count = BP_INITIAL_REFCOUNT;
            cb._ref_count_ex = BP_INITIAL_REFCOUNT;
            cb._clean_lsn = page->lsn;
            cb._page_lsn = page->lsn;

            // STEP 6) Fix successful -- pin page and downgrade latch
            // Just loaded a page, the page loading was due to:
            // User transaction - page was not involved in the recovery
            // operation
            // UNDO operation - the page is needed for b-tree search traversal
            // of an UNDO operation
            // REDO operation - on-demand or mixed REDO triggered by user
            // transaction page was in_doubt (using lock for concurrency
            // control) Page is safe to access, not calling
            // _validate_access(page) for page access validation purpose
            DBGOUT3(<<"bf_tree_m::_fix_nonswizzled: retrieved a new page: " << shpid);
            cb.pin();
            w_assert1(cb.latch().held_by_me());
            w_assert1(cb._pin_cnt > 0);
            DBG(<< "Fixed " << idx << " pin count " << cb._pin_cnt);

            if (mode != LATCH_EX) {
                // CS: I dont know what LATCh_Q is about
                // Ignoring for now
                w_assert1(mode == LATCH_SH);
                cb.latch().downgrade();
            }

            return RCOK;
        }
        else
        {
            // Page index is registered in hash table
            bf_tree_cb_t &cb = get_cb(idx);

            // Page is registered in hash table and it is not an in_doubt page,
            // meaning the actual page is in buffer pool already

            W_DO(cb.latch().latch_acquire(mode, conditional ?
                    sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));

            if (cb._pin_cnt < 0 || cb._pid != shpid)
            {
                // Page was evicted between hash table probe and latching
                DBG(<< "Page evicted right before latching. Retrying.");
                cb.latch().latch_release();
                continue;
            }

            if (cb._ref_count < BP_MAX_REFCOUNT) {
                ++cb._ref_count;
		// xum
		if (cb._ref_count-1 < 800 && cb._ref_count >= 800) {
			// New hot page
			hp_file.write((const char*)&(shpid),sizeof(int));
		}
            }
            if (mode == LATCH_EX && cb._ref_count_ex) {
                ++cb._ref_count_ex;
            }

            page = &(_buffer[idx]);

            // Page was already loaded in buffer pool by:
            //     Recovery operation - was an in_doubt page
            // or
            //     Previous user transaction - was not an in_doubt page
            // we need to validate the page only if running in concurrent mode
            // and the request coming from a user transaction
            //
            // Two scenarios:
            //     User transaction - validate
            //     Recovery operation - allow
            //                    from_recovery - UNDO operation
            //                    cb._recovery_access - REDO operation
            //         Two different ways to indicate caller from recovery, because
            //         REDO is page driven so we can mark cb._recovery_access
            //         but UNDO is transaction driven, not able to associate it to target
            //         page and also it has to traversal B-tree (visit multiple pages), so
            //         it is relying on input parameter 'from_recovery'

            w_assert1(cb.latch().held_by_me());
            cb.pin();
            // CS TODO: race condition! assert succeeds, but 0 is printed below
            w_assert1(cb._pin_cnt > 0);
            DBG(<< "Fix set pin cnt of " << idx << " to " << cb._pin_cnt);
            w_assert1(cb._pin_cnt > 0);

	    //stringstream stream;
	    //stream << "Page " << shpid << ": " << cb._ref_count << endl;
	    //cout << stream.str();
            return RCOK;
        }
    }
}

bf_idx bf_tree_m::pin_for_refix(const generic_page* page) {
    w_assert1(page != NULL);
    w_assert1(latch_mode(page) != LATCH_NL);

    bf_idx idx = page - _buffer;
    w_assert1(_is_active_idx(idx));

#ifdef SIMULATE_MAINMEMORYDB
    if (true) return idx;
#endif // SIMULATE_MAINMEMORYDB

    w_assert1(get_cb(idx)._pin_cnt >= 0);
    w_assert1(get_cb(idx).latch().held_by_me());

    get_cb(idx).pin();
    DBG(<< "Refix set pin cnt to " << get_cb(idx)._pin_cnt);
    return idx;
}

void bf_tree_m::unpin_for_refix(bf_idx idx) {
    w_assert1(_is_active_idx(idx));

#ifdef SIMULATE_MAINMEMORYDB
    if (true) return;
#endif // SIMULATE_MAINMEMORYDB

    w_assert1(get_cb(idx)._pin_cnt > 0);

    // CS TODO: assertion below fails when btcursor is destructed.  Therefore,
    // we are violating the rule that pin count can only be updated when page
    // is latched. But it seems that the program logic avoids something bad
    // happened. Still, it's quite edgy at the moment. I should probaly study
    // the btcursor code in detail before taking further action on this.
    // w_assert1(get_cb(idx).latch().held_by_me());
    get_cb(idx).unpin();
    DBG(<< "Unpin for refix set pin cnt to " << get_cb(idx)._pin_cnt);
    w_assert1(get_cb(idx)._pin_cnt >= 0);
}

///////////////////////////////////   Page fix/unfix END         ///////////////////////////////////

void bf_tree_m::switch_parent(PageID pid, generic_page* parent)
{
    bf_idx_pair p;
    bool found = _hashtable->lookup(pid, p);
    // if page is not cached, there is nothing to update
    if (!found) { return; }

    bf_idx parent_idx = parent - _buffer;
    // TODO CS: this assertion fails sometimes -- why?
    // Is it because of concurrent adoptions which race and one wins and the
    // other simply does a "dummy" adoption?
    // w_assert1(parent_idx != p.second);
    // ERROUT(<< "Parent of " << pid << " updated to " << parent_idx
    //         << " from " << p.second);
    p.second = parent_idx;
    found = _hashtable->update(pid, p);

    // page cannot be evicted since first lookup because caller latched it
    w_assert0(found);
}

void bf_tree_m::_convert_to_disk_page(generic_page* page) const {
    DBGOUT3 (<< "converting the page " << page->pid << "... ");

    fixable_page_h p;
    p.fix_nonbufferpool_page(page);
    int max_slot = p.max_child_slot();
    for (int i= -1; i<=max_slot; i++) {
        _convert_to_pageid(p.child_slot_address(i));
    }
}

void bf_tree_m::_convert_to_pageid (PageID* shpid) const {
    if ((*shpid) & SWIZZLED_PID_BIT) {
        bf_idx idx = (*shpid) ^ SWIZZLED_PID_BIT;
        w_assert1(_is_active_idx(idx));
        bf_tree_cb_t &cb = get_cb(idx);
        DBGOUT3 (<< "_convert_to_pageid(): converted a swizzled pointer bf_idx=" << idx << " to page-id=" << cb._pid);
        *shpid = cb._pid;
    }
}

general_recordid_t bf_tree_m::find_page_id_slot(generic_page* page, PageID shpid) const {
    w_assert1((shpid & SWIZZLED_PID_BIT) == 0);

    fixable_page_h p;
    p.fix_nonbufferpool_page(page);
    int max_slot = p.max_child_slot();

    //for (int i = -1; i <= max_slot; ++i) {
    for (general_recordid_t i = GeneralRecordIds::FOSTER_CHILD; i <= max_slot; ++i) {
        // ERROUT(<< "Looking for child " << shpid << " on " <<
        //         *p.child_slot_address(i));
        if (*p.child_slot_address(i) == shpid) {
            // ERROUT(<< "OK");
            return i;
        }
    }
    return GeneralRecordIds::INVALID;
}

///////////////////////////////////   SWIZZLE/UNSWIZZLE BEGIN ///////////////////////////////////

void bf_tree_m::swizzle_child(generic_page* parent, general_recordid_t slot)
{
    return swizzle_children(parent, &slot, 1);
}

void bf_tree_m::swizzle_children(generic_page* parent, const general_recordid_t* slots,
                                 uint32_t slots_size) {
    w_assert1(is_swizzling_enabled());
    w_assert1(parent != NULL);
    w_assert1(latch_mode(parent) != LATCH_NL);
    w_assert1(_is_active_idx(parent - _buffer));
    w_assert1(is_swizzled(parent)); // swizzling is transitive.

    fixable_page_h p;
    p.fix_nonbufferpool_page(parent);
    for (uint32_t i = 0; i < slots_size; ++i) {
        general_recordid_t slot = slots[i];
        // To simplify the tree traversal while unswizzling,
        // we never swizzle foster-child pointers.
        w_assert1(slot >= GeneralRecordIds::PID0); // was w_assert1(slot >= -1);
        w_assert1(slot <= p.max_child_slot());

        PageID* addr = p.child_slot_address(slot);
        if (((*addr) & SWIZZLED_PID_BIT) == 0) {
            _swizzle_child_pointer(parent, addr);
        }
    }
}

void bf_tree_m::_swizzle_child_pointer(generic_page* parent, PageID* pointer_addr) {
    PageID pid = *pointer_addr;
    //w_assert1((child_shpid & SWIZZLED_PID_BIT) == 0);
    bf_idx_pair p;
    bf_idx idx = 0;
    if (_hashtable->lookup(pid, p)) {
        idx = p.first;
    }
    // so far, we don't swizzle a child page if it's not in bufferpool yet.
    if (idx == 0) {
        DBGOUT1(<< "Unexpected! the child page " << pid << " isn't in bufferpool yet. gave up swizzling it");
        // this is still okay. swizzling is best-effort
        return;
    }
    bool concurrent_swizzling = false;
    while (!lintel::unsafe::atomic_compare_exchange_strong(const_cast<bool*>(&(get_cb(idx)._concurrent_swizzling)), &concurrent_swizzling, true))
    { }

    if ((pid & SWIZZLED_PID_BIT) != 0) {
        /* another thread swizzled it for us */
        get_cb(idx)._concurrent_swizzling = false;
        return;
    }

    // To swizzle the child, add a pin on the page.
    // We might fail here in a very unlucky case.  Still, it's fine.
    // bool pinned = _increment_pin_cnt_no_assumption (idx);
    // if (!pinned) {
    //     DBGOUT1(<< "Unlucky! the child page " << child_shpid << " has been just evicted. gave up swizzling it");
    //     get_cb(idx)._concurrent_swizzling = false;
    //     return;
    // }

    // we keep the pin until we unswizzle it.
    *pointer_addr = idx | SWIZZLED_PID_BIT; // overwrite the pointer in parent page.
    get_cb(idx)._swizzled = true;
#ifdef BP_TRACK_SWIZZLED_PTR_CNT
    get_cb(parent)->_swizzled_ptr_cnt_hint++;
#endif
    ++_swizzled_page_count_approximate;
    get_cb(idx)._concurrent_swizzling = false;
}

bool bf_tree_m::_are_there_many_swizzled_pages() const {
    return _swizzled_page_count_approximate >= (int) (_block_cnt * 2 / 10);
}

bool bf_tree_m::has_swizzled_child(bf_idx node_idx) {
    fixable_page_h node_p;
    node_p.fix_nonbufferpool_page(_buffer + node_idx);
    int max_slot = node_p.max_child_slot();
    // skipping foster pointer...
    for (int32_t j = 0; j <= max_slot; ++j) {
        PageID shpid = *node_p.child_slot_address(j);
        if ((shpid & SWIZZLED_PID_BIT) != 0) {
            return true;
        }
    }
    return false;
}

w_rc_t bf_tree_m::load_for_redo(bf_idx idx,
                  PageID shpid)
{
    // Special function for Recovery REDO phase
    // idx is in hash table already
    // but the actual page has not loaded from disk into buffer pool yet

    // Caller of this fumction is responsible for acquire and release EX latch on this page

    w_rc_t rc = RCOK;
    w_assert1(shpid != 0);

    DBGOUT3(<<"REDO phase: loading page " << shpid
            << " into buffer pool frame " << idx);

    // Load the physical page from disk
    W_DO(smlevel_0::vol->read_page(shpid, &_buffer[idx]));

    // For the loaded page, compare its checksum
    // If inconsistent, return error
    if (_buffer[idx].checksum != 0) {
        uint32_t checksum = _buffer[idx].calculate_checksum();
        if (checksum != _buffer[idx].checksum)
        {
            ERROUT(<<"bf_tree_m: bad page checksum in page " << shpid
                    << " -- expected " << checksum
                    << " got " << _buffer[idx].checksum);
            return RC (eBADCHECKSUM);
        }
    }

    // Then, page ID must match, otherwise raise error
    if (shpid != _buffer[idx].pid) {
        W_FATAL_MSG(eINTERNAL, <<"inconsistent disk page: "
                << "." << shpid << " was " << _buffer[idx].pid);
    }

    return rc;
}

struct latch_auto_release {
    latch_auto_release (latch_t &latch) : _latch(latch) {}
    ~latch_auto_release () {
        _latch.latch_release();
    }
    latch_t &_latch;
};

bool bf_tree_m::_unswizzle_a_frame(bf_idx parent_idx, uint32_t child_slot) {
    // misc checks. if any check fails, just returns false.
    bf_tree_cb_t &parent_cb = get_cb(parent_idx);
    if (!parent_cb._used) {
        return false;
    }
    if (!parent_cb._swizzled) {
        return false;
    }
    // now, try a conditional latch on parent page.
    w_rc_t latch_rc = parent_cb.latch().latch_acquire(LATCH_EX, sthread_t::WAIT_IMMEDIATE);
    if (latch_rc.is_error()) {
        DBGOUT2(<<"_unswizzle_a_frame: oops, unlucky. someone is latching this page. skipiing this. rc=" << latch_rc);
        return false;
    }
    latch_auto_release auto_rel(parent_cb.latch()); // this automatically releaes the latch.

    fixable_page_h parent;
    parent.fix_nonbufferpool_page(_buffer + parent_idx);
    if (child_slot >= (uint32_t) parent.max_child_slot()+1) {
        return false;
    }
    PageID* shpid_addr = parent.child_slot_address(child_slot);
    PageID shpid = *shpid_addr;
    if ((shpid & SWIZZLED_PID_BIT) == 0) {
        return false;
    }
    bf_idx child_idx = shpid ^ SWIZZLED_PID_BIT;
    bf_tree_cb_t &child_cb = get_cb(child_idx);
    w_assert1(child_cb._used);
    w_assert1(child_cb._swizzled);
    // in some lazy testcases, _buffer[child_idx] aren't initialized. so these checks are disabled.
    // see the above comments on cache miss
    // w_assert1(child_cb._pid == _buffer[child_idx].pid);
    // w_assert1(_buffer[child_idx].btree_level == 1);
    w_assert1(child_cb._pin_cnt >= 1); // because it's swizzled
    bf_idx_pair p;
    w_assert1(_hashtable->lookup(child_cb._pid, p));
    w_assert1(child_idx == p.first);
    child_cb._swizzled = false;
#ifdef BP_TRACK_SWIZZLED_PTR_CNT
    if (parent_cb._swizzled_ptr_cnt_hint > 0) {
        parent_cb._swizzled_ptr_cnt_hint--;
    }
#endif
    // because it was swizzled, the current pin count is >= 1, so we can simply do atomic decrement.
    // _decrement_pin_cnt_assume_positive(child_idx);
    --_swizzled_page_count_approximate;

    *shpid_addr = child_cb._pid;
    w_assert1(((*shpid_addr) & SWIZZLED_PID_BIT) == 0);

    return true;
}

///////////////////////////////////   SWIZZLE/UNSWIZZLE END ///////////////////////////////////

void bf_tree_m::debug_dump(std::ostream &o) const
{
    o << "dumping the bufferpool contents. _block_cnt=" << _block_cnt << "\n";
    o << "  _freelist_len=" << _freelist_len << ", HEAD=" << FREELIST_HEAD << "\n";

    for (uint32_t store = 1; store < stnode_page::max; ++store) {
        if (_root_pages[store] != 0) {
            o << ", " << store << "=" << _root_pages[store];
        }
    }
    o << std::endl;
    for (bf_idx idx = 1; idx < _block_cnt && idx < 1000; ++idx) {
        o << "  frame[" << idx << "]:";
        bf_tree_cb_t &cb = get_cb(idx);
        if (cb._used) {
            o << "page-" << cb._pid;
            if (cb.is_dirty()) {
                o << " (dirty)";
            }
            o << ", _swizzled=" << cb._swizzled;
            o << ", _pin_cnt=" << cb._pin_cnt;
            o << ", _ref_count=" << cb._ref_count;
            o << ", _ref_count_ex=" << cb._ref_count_ex;
            o << ", ";
            cb.latch().print(o);
        } else {
            o << "unused (next_free=" << _freelist[idx] << ")";
        }
        o << std::endl;
    }
    if (_block_cnt >= 1000) {
        o << "  ..." << std::endl;
    }
}

void bf_tree_m::debug_dump_page_pointers(std::ostream& o, generic_page* page) const {
    bf_idx idx = page - _buffer;
    w_assert1(idx > 0);
    w_assert1(idx < _block_cnt);

    o << "dumping page:" << page->pid << ", bf_idx=" << idx << std::endl;
    o << "  ";
    fixable_page_h p;
    p.fix_nonbufferpool_page(page);
    for (int i= -1; i<=p.max_child_slot(); i++) {
        if (i > -1) {
            o << ", ";
        }
        o << "child[" << i << "]=";
        debug_dump_pointer(o, *p.child_slot_address(i));
    }
    o << std::endl;
}
void bf_tree_m::debug_dump_pointer(ostream& o, PageID shpid) const
{
    if (shpid & SWIZZLED_PID_BIT) {
        bf_idx idx = shpid ^ SWIZZLED_PID_BIT;
        o << "swizzled(bf_idx=" << idx;
        o << ", page=" << get_cb(idx)._pid << ")";
    } else {
        o << "normal(page=" << shpid << ")";
    }
}

PageID bf_tree_m::debug_get_original_pageid (PageID shpid) const {
    if (is_swizzled_pointer(shpid)) {
        bf_idx idx = shpid ^ SWIZZLED_PID_BIT;
        return get_cb(idx)._pid;
    } else {
        return shpid;
    }
}

w_rc_t bf_tree_m::set_swizzling_enabled(bool enabled) {
    if (_enable_swizzling == enabled) {
        return RCOK;
    }
    DBGOUT1 (<< "changing the pointer swizzling setting in the bufferpool to " << enabled << "... ");

    // changing this setting affects all buffered pages because
    //   swizzling-off : "_parent" in the control block is not set
    //   swizzling-on : "_parent" in the control block is set

    // first, flush out all dirty pages. we assume there is no concurrent transaction
    // which produces dirty pages from here on. if there is, booomb.
    _cleaner->wakeup(true);

    // clear all properties. could call uninstall_volume for each of them,
    // but nuking them all is faster.
    for (bf_idx i = 0; i < _block_cnt; i++) {
        bf_tree_cb_t &cb = get_cb(i);
        cb.clear();
    }
    _freelist[0] = 1;
    for (bf_idx i = 1; i < _block_cnt - 1; ++i) {
        _freelist[i] = i + 1;
    }
    _freelist[_block_cnt - 1] = 0;
    _freelist_len = _block_cnt - 1; // -1 because [0] isn't a valid block

    //re-create hashtable
    delete _hashtable;
    int buckets = w_findprime(1024 + (_block_cnt / 4));
    _hashtable = new bf_hashtable<bf_idx_pair>(buckets);
    w_assert0(_hashtable != NULL);

    // finally switch the property
    _enable_swizzling = enabled;

    DBGOUT1 (<< "changing the pointer swizzling setting done. ");
    return RCOK;
}


w_rc_t bf_tree_m::_sx_update_child_emlsn(btree_page_h &parent, general_recordid_t child_slotid,
                                         lsn_t child_emlsn) {
    sys_xct_section_t sxs (true); // this transaction will output only one log!
    W_DO(sxs.check_error_on_start());
    w_assert1(parent.is_latched());
    W_DO(log_page_evict(parent, child_slotid, child_emlsn));
    parent.set_emlsn_general(child_slotid, child_emlsn);
    W_DO (sxs.end_sys_xct (RCOK));
    return RCOK;
}

void bf_tree_m::_delete_block(bf_idx idx) {
    w_assert1(_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1(cb._pin_cnt == 0);
    w_assert1(!cb.latch().is_latched());
    cb._used = false; // clear _used BEFORE _dirty so that eviction thread will ignore this block.

    DBGOUT1(<<"delete block: remove page shpid = " << cb._pid);
    bool removed = _hashtable->remove(cb._pid);
    w_assert1(removed);

    // after all, give back this block to the freelist. other threads can see this block from now on
    _add_free_block(idx);
}

bf_tree_cb_t* bf_tree_m::get_cbp(bf_idx idx) const {
#ifdef BP_ALTERNATE_CB_LATCH
    bf_idx real_idx;
    real_idx = (idx << 1) + (idx & 0x1); // more efficient version of: real_idx = (idx % 2) ? idx*2+1 : idx*2
    return &_control_blocks[real_idx];
#else
    return &_control_blocks[idx];
#endif
}

bf_tree_cb_t& bf_tree_m::get_cb(bf_idx idx) const {
    return *get_cbp(idx);
}

bf_idx bf_tree_m::get_idx(const bf_tree_cb_t* cb) const {
    bf_idx real_idx = cb - _control_blocks;
#ifdef BP_ALTERNATE_CB_LATCH
    return real_idx / 2;
#else
    return real_idx;
#endif
}

bf_tree_cb_t* bf_tree_m::get_cb(const generic_page *page) {
    bf_idx idx = page - _buffer;
    w_assert1(_is_valid_idx(idx));
    return get_cbp(idx);
}

generic_page* bf_tree_m::get_page(const bf_tree_cb_t *cb) {
    bf_idx idx = get_idx(cb);
    w_assert1(_is_valid_idx(idx));
    return _buffer + idx;
}

generic_page* bf_tree_m::get_page(const bf_idx& idx) {
    w_assert1(_is_valid_idx(idx));
    return _buffer + idx;
}

PageID bf_tree_m::get_root_page_id(StoreID store) {
    bf_idx idx = _root_pages[store];
    if (!_is_valid_idx(idx)) {
        return 0;
    }
    generic_page* page = _buffer + idx;
    return page->pid;
}

bf_idx bf_tree_m::get_root_page_idx(StoreID store) {
    // root-page index is always kept in the volume descriptor:
    bf_idx idx = _root_pages[store];
    if (!_is_valid_idx(idx))
        return 0;
    else
        return idx;
}

///////////////////////////////////   Page fix/unfix BEGIN         ///////////////////////////////////

const uint32_t SWIZZLED_LRU_UPDATE_INTERVAL = 1000;

w_rc_t bf_tree_m::refix_direct (generic_page*& page, bf_idx
                                       idx, latch_mode_t mode, bool conditional) {
    bf_tree_cb_t &cb = get_cb(idx);
    W_DO(cb.latch().latch_acquire(mode, conditional ?
                sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));
    w_assert1(cb._pin_cnt > 0);
    cb.pin();
    DBG(<< "Refix direct of " << idx << " set pin cnt to " << cb._pin_cnt);
    ++cb._ref_count;
    //xum
    if (cb._ref_count -1 < 800 && cb._ref_count >= 800) {
	// New hot page
	hp_file.write((const char*)&(page->pid),sizeof(int));
    }
    if (mode == LATCH_EX) { ++cb._ref_count_ex; }
    page = &(_buffer[idx]);
    return RCOK;
}

w_rc_t bf_tree_m::fix_nonroot(generic_page*& page, generic_page *parent,
                                     PageID shpid, latch_mode_t mode, bool conditional,
                                     bool virgin_page, lsn_t emlsn)
{
    INC_TSTAT(bf_fix_nonroot_count);
    return _fix_nonswizzled(parent, page, shpid, mode, conditional, virgin_page, emlsn);
}

w_rc_t bf_tree_m::fix_root (generic_page*& page, StoreID store,
                                   latch_mode_t mode, bool conditional,
                                   bool virgin)
{
    w_assert1(store != 0);

    // root-page index is always kept in the volume descriptor:
    bf_idx idx = _root_pages[store];
    if (!_is_valid_idx(idx)) {
        // Load root page
        PageID root_pid = smlevel_0::vol->get_store_root(store);
        W_DO(_fix_nonswizzled(NULL, page, root_pid, mode, conditional, virgin));

        bf_idx_pair p;
        bool found = _hashtable->lookup(root_pid, p);
        w_assert0(found);
        idx = p.first;
        _root_pages[store] = idx;
    }
    else {
        // skip hash table lookup
        // (this is the only optimization currently applied to root pages)
        W_DO(get_cb(idx).latch().latch_acquire(
                    mode, conditional ? sthread_t::WAIT_IMMEDIATE : sthread_t::WAIT_FOREVER));
        page = &(_buffer[idx]);
    }

    w_assert1(_is_valid_idx(idx));
    w_assert1(_is_active_idx(idx));
    w_assert1(get_cb(idx)._used);

    get_cb(idx).pin();
    w_assert1(get_cb(idx)._pin_cnt > 0);
    w_assert1(get_cb(idx).latch().held_by_me());
    DBG(<< "Fixed root " << idx << " pin cnt " << get_cb(idx)._pin_cnt);
    return RCOK;
}


void bf_tree_m::unfix(const generic_page* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1(cb.latch().held_by_me());
    cb.unpin();
    w_assert1(cb._pin_cnt >= 0);
    DBG(<< "Unfixed " << idx << " pin count " << cb._pin_cnt);
    cb.latch().latch_release();
}

bool bf_tree_m::is_dirty(const generic_page* p) const {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx).is_dirty();
}

bool bf_tree_m::is_dirty(const bf_idx idx) const {
    // Caller has latch on page
    // Used by REDO phase in Recovery
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx).is_dirty();
}

bool bf_tree_m::is_used (bf_idx idx) const {
    return _is_active_idx(idx);
}

void bf_tree_m::set_page_lsn(generic_page* p, lsn_t lsn)
{
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    get_cb(idx).set_page_lsn(lsn);
}

lsn_t bf_tree_m::get_page_lsn(generic_page* p)
{
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx).get_page_lsn();
}

latch_mode_t bf_tree_m::latch_mode(const generic_page* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx).latch().mode();
}

void bf_tree_m::downgrade_latch(const generic_page* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1(cb.latch().held_by_me());
    cb.latch().downgrade();
}

bool bf_tree_m::upgrade_latch_conditional(const generic_page* p) {
    uint32_t idx = p - _buffer;
    w_assert1 (_is_active_idx(idx));
    bf_tree_cb_t &cb = get_cb(idx);
    w_assert1(cb.latch().held_by_me());
    if (cb.latch().mode() == LATCH_EX) {
        return true;
    }
    bool would_block = false;
    cb.latch().upgrade_if_not_block(would_block);
    if (!would_block) {
        w_assert1 (cb.latch().mode() == LATCH_EX);
        return true;
    } else {
        return false;
    }
}

void print_latch_holders(latch_t* latch);


///////////////////////////////////   Page fix/unfix END         ///////////////////////////////////

///////////////////////////////////   LRU/Freelist BEGIN ///////////////////////////////////
#ifdef BP_MAINTAIN_PARENT_PTR
// bool bf_tree_m::_is_in_swizzled_lru (bf_idx idx) const {
//     w_assert1 (_is_active_idx(idx));
//     return SWIZZLED_LRU_NEXT(idx) != 0 || SWIZZLED_LRU_PREV(idx) != 0 || SWIZZLED_LRU_HEAD == idx;
// }
#endif // BP_MAINTAIN_PARENT_PTR
bool bf_tree_m::is_swizzled(const generic_page* page) const
{
    bf_idx idx = page - _buffer;
    w_assert1 (_is_active_idx(idx));
    return get_cb(idx)._swizzled;
}

///////////////////////////////////   LRU/Freelist END ///////////////////////////////////

bool bf_tree_m::_is_valid_idx(bf_idx idx) const {
    return idx > 0 && idx < _block_cnt;
}


bool bf_tree_m::_is_active_idx (bf_idx idx) const {
    return _is_valid_idx(idx) && get_cb(idx)._used;
}


void pin_for_refix_holder::release() {
    if (_idx != 0) {
        smlevel_0::bf->unpin_for_refix(_idx);
        _idx = 0;
    }
}


PageID bf_tree_m::normalize_shpid(PageID shpid) const {
    generic_page* page;
#ifdef SIMULATE_MAINMEMORYDB
    bf_idx idx = shpid;
    page = &_buffer[idx];
    return page->pid;
#else
    // if (is_swizzling_enabled()) {
        if (is_swizzled_pointer(shpid)) {
            bf_idx idx = shpid ^ SWIZZLED_PID_BIT;
            page = &_buffer[idx];
            return page->pid;
        }
    // }
    return shpid;
#endif
}

bool pidCmp(const PageID& a, const PageID& b) { return a < b; }

void WarmupThread::fixChildren(btree_page_h& parent, size_t& fixed, size_t max)
{
    btree_page_h page;
    if (parent.get_foster() > 0) {
        page.fix_nonroot(parent, parent.get_foster(), LATCH_SH);
        fixed++;
        fixChildren(page, fixed, max);
        page.unfix();
    }

    if (parent.is_leaf()) {
        return;
    }

    page.fix_nonroot(parent, parent.pid0(), LATCH_SH);
    fixed++;
    fixChildren(page, fixed, max);
    page.unfix();

    size_t nrecs = parent.nrecs();
    for (size_t j = 0; j < nrecs; j++) {
        if (fixed >= max) {
            return;
        }
        PageID pid = parent.child(j);
        page.fix_nonroot(parent, pid, LATCH_SH);
        fixed++;
        fixChildren(page, fixed, max);
        page.unfix();
    }
}

// xum
void WarmupThread::fixHot(int length, size_t& fixed,char* buffer)
{
    btree_page_h page;
    for(int i=0;i<length;i+=4) {
	int* pid = (int*)(buffer+i);
	page.fix_direct(*pid,LATCH_SH);
	fixed++;
    }
}

void WarmupThread::run()
{
    cout << "[XUM] Begin warmup by preloading hot pages ..." << endl;
    size_t npages = smlevel_0::bf->get_size();
    vol_t* vol = smlevel_0::vol;
    stnode_cache_t* stcache = vol->get_stnode_cache();
    vector<StoreID> stids;
    stcache->get_used_stores(stids);

    //xum: preload hot pages
    ifstream inf("hot_pages.dump",ios::binary|ios::in);
    inf.seekg (0, inf.end);
    int length = inf.tellg();
    inf.seekg (0, inf.beg);

    char * buffer = new char [length];
    inf.read(buffer,length);

    // Load all pages in depth-first until buffer is full or all pages read
    btree_page_h parent;
    vector<PageID> pids;
    size_t fixed = 0;
    for (size_t i = 0; i < stids.size(); i++) {
        parent.fix_root(stids[i], LATCH_SH);
        fixed++;
        //fixChildren(parent, fixed, npages);
	fixHot(length,fixed,buffer);
    }
    delete [] buffer;  	

    ERROUT(<< "Finished warmup! Pages fixed: " << fixed << " of " << npages <<
            " with DB size " << vol->get_alloc_cache()->get_last_allocated_pid());
}
