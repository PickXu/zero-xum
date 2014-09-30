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

/*<std-header orig-src='shore' incl-file-exclusion='SRV_LOG_H'>

 $Id: log_core.h,v 1.11 2010/09/21 14:26:19 nhall Exp $

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

#ifndef LOG_CORE_H
#define LOG_CORE_H
#include "w_defines.h"

/*  -- do not edit anything above this line --   </std-header>*/

#include <Lintel/AtomicCounter.hpp>
#include <log.h>
#include <vector> // only for _collect_single_page_recovery_logs()

// in sm_base for the purpose of log callback function argument type
typedef    smlevel_0::partition_number_t partition_number_t; 
enum       { PARTITION_COUNT= smlevel_0::max_openlog };
class      partition_t ; // forward
typedef    int    partition_index_t;

#define CHKPT_META_BUF 512

class skip_log; // forward
class ConsolidationArray;
struct CArraySlot;

#include <partition.h>
#include <deque>
#include "mcs_lock.h"
#include "tatas.h"

// LOG_BUFFER switch
#include "logbuf_common.h"
#include "logbuf_core.h"


/**
 * \brief Core Implementation of Log Manager
 * \ingroup SSMLOG
 * \details
 * This is the internal implementation class used from log_m.
 * This class contains the dirty details which should not be exposed to other modules.
 * It is similar to what people call "pimpl" or "compiler firewall".
 * @see log_m
 */
class log_core : public log_m
{
    //#ifdef LOG_BUFFER
    friend class logbuf_core;
    //#endif

    struct waiting_xct {
        fileoff_t* needed;
        pthread_cond_t* cond;
        NORET waiting_xct(fileoff_t *amt, pthread_cond_t* c)
            : needed(amt), cond(c)
        {
        }
    };
    static std::deque<log_core::waiting_xct*> _log_space_waiters;

#ifdef LOG_BUFFER
public:
    logbuf_core *_log_buffer; // the new log buffer
#endif

private:
    static bool          _initialized;
    bool                 _reservations_active;

#ifdef LOG_BUFFER
    // moved to logbuf_core
#else
    bool _waiting_for_space; // protected by log_m::_insert_lock/_wait_flush_lock
    bool _waiting_for_flush; // protected by log_m::_wait_flush_lock
#endif

    enum { invalid_fhdl = -1 };

#ifdef LOG_BUFFER
    long                 start_byte() const { return _log_buffer->_start; } 
    long                 end_byte() const { return _log_buffer->_end; } 

    long                 _segsize; // log buffer size
public:
    long                 segsize() const { return _segsize; }
private:
    lsn_t                _flush_lsn;

#else
    long _start; // byte number of oldest unwritten byte
    long                 start_byte() const { return _start; } 

    long _end; // byte number of insertion point
    long                 end_byte() const { return _end; } 

    long                 _segsize; // log buffer size
    long                 segsize() const { return _segsize; }
    // long                 _blocksize; uses constant BLOCK_SIZE
    char*                _buf; // log buffer: _segsize buffer into which
                         // inserts copy log records with log_core::insert

    // Set of pointers into _buf (circular log buffer)
    // and associated lsns. See detailed comments at log_core::insert
    struct epoch {
        lsn_t base_lsn; // lsn of _buf[0] for this epoch

        long base; // absolute position of _buf[0] (absolute meaning
                   // relative to the beginning of log.1)
        long start; // offset from _buf[0] of this epoch
        long end;  // offset into log buffers _buf[0] of tail of
                   // log. Wraps modulo log buffer size, aka segsize.
        epoch()
            : base_lsn(lsn_t::null), base(0), start(0), end(0)
        {
        }
        epoch(lsn_t l, long b, long s, long e)
            : base_lsn(l), base(b), start(s), end(e)
        {
            w_assert1(e >= s);
        }
        epoch volatile* vthis() { return this; }
    };

    /** \ingroup CARRAY */
    epoch                _buf_epoch;
    epoch                _cur_epoch;
    epoch                _old_epoch;

    /*
     * See src/internals.h, section LOG_M_INTERNAL
    Divisions:

    Physical layout:

    The log consists of an unbounded number of "partitions" each
    consisting of a fixed number of "segments." A partition is the
    largest file that will be created and a segment is the size of the
    in-memory buffer. Segments are further divided into "blocks" which
    are the unit of I/O. 

    Threads insert "entries" into the log (log records). 

    One or more entries make up an "epoch" (data that will be flushed
    using a single I/O). Epochs normally end at the end of a segment.
    The log flush daemon constantly flushes any unflushed portion of
    "valid" epochs. (An epoch is valid if its end > start.)
    When an epoch reaches the end of a segment, the final log entry 
    will usually spill over into the next segment and the next 
    entry will begin a new epoch at a non-zero
    offset of the new segment. However, a log entry which would spill
    over into a new partition will begin a new epoch and join it.
    Log records do not span partitions. 
    */

    /* FRJ: Partitions are not protected by either the insert or flush
       mutex, but are instead managed separately using a combination
       of mutex and reference counts. We do this because read
       operations (e.g. fetch) need not impact either inserts or
       flushes because (by definition) we read only already-written
       data, which insert/flush never touches.

       Any time we change which file a partition_t points at (via open
       or close), we must acquire the partition mutex. Each call to
       open() increments a reference count which will be decremented
       by a matching call to close(). Once a partition is open threads
       may safely use it without the mutex because it will not be
       closed until the ref count goes to zero. In particular, log
       inserts do *not* acquire the partition mutex unless they need
       to change the curr_partition.

       A thread should always acquire the partition mutex last. This
       should happen naturally, since log_m acquires insert/flush
       mutexen and srv_log acquires the partition mutex.
     */

    lsn_t                _flush_lsn;

    /** @cond */ char    _padding[CACHELINE_SIZE]; /** @endcond */
    tatas_lock           _flush_lock;
    /** @cond */ char    _padding2[CACHELINE_TATAS_PADDING]; /** @endcond */
    tatas_lock           _comp_lock;
    /** @cond */ char    _padding3[CACHELINE_TATAS_PADDING]; /** @endcond */
    /** Lock to protect threads acquiring their log buffer. */
    mcs_lock             _insert_lock;
    /** @cond */ char    _padding4[CACHELINE_MCS_PADDING]; /** @endcond */

    // paired with _wait_cond, _flush_cond
    pthread_mutex_t      _wait_flush_lock; 
    pthread_cond_t       _wait_cond;  // paired with _wait_flush_lock
    pthread_cond_t       _flush_cond;  // paird with _wait_flush_lock

    sthread_t*           _flush_daemon;
    /// @todo both of the below should become std::atomic_flag's at some time
    lintel::Atomic<bool> _shutting_down;
    lintel::Atomic<bool> _flush_daemon_running; // for asserts only

    /**
     * Consolidation array for this log manager.
     * \ingroup CARRAY
     */
    ConsolidationArray*  _carray;
#endif // LOG_BUFFER


    // Data members:
    partition_index_t   _curr_index; // index of partition
    partition_number_t  _curr_num;   // partition number
    char*               _readbuf;
#ifdef LOG_DIRECT_IO
    char*               _writebuf;   // a temp buffer used by partition_t::flush to do alignment adjustment for direct IO
#endif

    skip_log*           _skip_log;

    pthread_mutex_t     _scavenge_lock;
    pthread_cond_t      _scavenge_cond;
    partition_t         _part[PARTITION_COUNT];
public:
    enum { BLOCK_SIZE=partition_t::XFERSIZE };
    // DO NOT MAKE SEGMENT_SIZE smaller than 3 pages!  Since we need to
    // fit at least a single max-sized log record in a segment.
    // It would make no sense whatsoever to make it that small.
    // TODO: we need a better way to parameterize this; if a page
    // is large, we don't necessarily want to force writes to be
    // large; but we do need to make the segment size some reasonable
    // number of pages. If pages are 32K, then 128 blocks is only
    // four pages, which will accommodate all log records .
    //
    // NOTE: we have to fit two checkpoints into a segment, and
    // the checkpoint size is a function of the number of buffers in
    // the buffer pool among other things; so a maximum-sized checkpoint
    // is pretty big and the smaller the page size, the bigger it is.
    // 128 pages is 32 32-K pages, which is room enough for
    // 10+ max-sized log records.
#if SM_PAGESIZE < 8192
    enum { SEGMENT_SIZE= 256 * BLOCK_SIZE };
#else
    //    enum { SEGMENT_SIZE= 128 * BLOCK_SIZE };
    // debugging
    enum { SEGMENT_SIZE= 128 * BLOCK_SIZE };    
#endif
    
    // CONSTRUCTOR 
    static rc_t    new_log_m(
                        log_m    *&the_log,
                        int        wrlogbufsize,
                        bool    reformat,
                        int        carray_active_slot_count
#ifdef LOG_BUFFER
                 ,
                 int logbuf_seg_count,
                 int logbuf_flush_trigger,
                 int logbuf_block_size
#endif

                             );

    // Exported to log_m
    static          log_core *THE_LOG;


    int             get_last_lsns(lsn_t* array);
    
    // exported to partition_t
    static void     destroy_file(partition_number_t n, bool e);
    char *          readbuf() { return _readbuf; }
#ifdef LOG_DIRECT_IO
    char *          writebuf() { return _writebuf; }
#endif

    // used by partition_t
    skip_log*       get_skip_log()  { return _skip_log; }

    NORET           log_core(
                             long bsize, // segment size for the log buffer, set through "sm_logbufsize"
                             bool reformat,
                             int carray_active_slot_count
#ifdef LOG_BUFFER
                             ,
                             int logbuf_seg_count,
                             int logbuf_flush_trigger,
                             int logbuf_block_size
#endif
                             );
    NORET           ~log_core();
    // do whatever needs to be done before destructor is callable
    void            shutdown(); 

    // for log_m :
    int             segment_size() const { return SEGMENT_SIZE; }

    // returns lsn where data were written 
    rc_t            insert(logrec_t &r, lsn_t* l); 
    rc_t            flush(const lsn_t &lsn, bool block=true, bool signal=true, bool *ret_flushed=NULL);
    rc_t            compensate(const lsn_t &orig_lsn, const lsn_t& undo_lsn);
    void            start_flush_daemon();
#ifdef LOG_BUFFER
    rc_t            fetch(lsn_t &lsn, logrec_t* &rec, lsn_t* nxt, const bool forward);
    rc_t            fetch(lsn_t &lsn, logrec_t* &rec, lsn_t* nxt, hints_op op);
#else
    rc_t            fetch(lsn_t &lsn, logrec_t* &rec, lsn_t* nxt, const bool forward);
#endif
    rc_t            scavenge(const lsn_t &min_rec_lsn, const lsn_t&min_xct_lsn);
    void            release_space(fileoff_t howmuch);
    void            activate_reservations() ;
    fileoff_t       consume_chkpt_reservation(fileoff_t howmuch);
    rc_t            wait_for_space(fileoff_t &amt, timeout_in_ms timeout);
    bool            reservations_active() const { return _reservations_active; }
    rc_t            file_was_archived(const char * /*file*/);

    /* Q: how much reservable space does scavenging pcount partitions
          give back?
       
      A: everything except a bit we have to keep to ensure the log
          can always be flushed.
     */
    size_t              recoverable_space(int pcount) const {
        // NEH: substituted BLOCK_SIZE for writebufsize()/PARTITION_COUNT
        // just a guess; writebufsize is no more...
                               return pcount*(_partition_data_size - BLOCK_SIZE);
                            }

#ifdef LOG_BUFFER
    // moved to logbuf_core
#else
    // for flush_daemon_thread_t
    void            flush_daemon();
    lsn_t           flush_daemon_work(lsn_t old_mark);
#endif


private:
#ifdef LOG_BUFFER
    partition_t *_flushX_get_partition(lsn_t start_lsn, 
                                       long start1, long end1, long start2, 
                                       long end2);
    // _flushX is moved to logbuf_core
#else
    void            _flushX(lsn_t base_lsn, long start1, 
                              long end1, long start2, long end2);
#endif
    w_rc_t          _set_size(fileoff_t psize);
    fileoff_t       _get_min_size() const {
                        // Return minimum log size as a function of the
                        // log buffer size and the partition count
                        return ( segsize()  + BLOCK_SIZE) * PARTITION_COUNT;
                    }
    partition_t *   _partition(partition_index_t i) const;


#ifdef LOG_BUFFER
    // moved to logbuf_core
#else
    /**
     * \ingroup CARRAY
     *  @{
     */
    void _acquire_buffer_space(CArraySlot* info, long size);
    lsn_t _copy_to_buffer(logrec_t &rec, long pos, long size, CArraySlot* info);
    bool _update_epochs(CArraySlot* info);
    /** @}*/
#endif

public:
    // for partition_t
    void                unset_current(); 
    void                set_current(partition_index_t, partition_number_t); 
    partition_index_t   partition_index() const { return _curr_index; }
    partition_number_t  partition_num() const { return _curr_num; }
    static long         floor2(long offset, long block_size) 
                            { return offset & -block_size; }
    static long         ceil2(long offset, long block_size) 
                           { return 
                               floor2(offset + block_size - 1, block_size); }
    static long         partition_size(long psize);
    static long         min_partition_size();

    // exposted to partition_t:
    static long         prime(char* buf, int fd, fileoff_t start, lsn_t next);
private:
    void                _prime(int fd, fileoff_t start, lsn_t next); 
    static long         _floor(long offset, long block_size) 
                            { return (offset/block_size)*block_size; }
    static long         _ceil(long offset, long block_size) 
                            { return _floor(offset + block_size - 1, block_size); }


public:
    void   release(); 
private:
    void   _acquire();
public:


private:
    // helper for _open()
    partition_t *       _close_min(partition_number_t n);
                                // the defaults are for the case
                                // in which we're opening a file to 
                                // be the new "current"
    partition_t *       _open_partition(partition_number_t n, 
                            const lsn_t&  end_hint,
                            bool existing,
                            bool forappend,
                            bool during_recovery
                        ); 
    partition_t *       _open_partition_for_append(partition_number_t n, 
                            const lsn_t&  end_hint,
                            bool existing,
                            bool during_recovery
                        ) { return _open_partition(n,  
                                    end_hint, existing,
                                    true, during_recovery); 
                          }
    partition_t *       _open_partition_for_read(partition_number_t n, 
                            const lsn_t&  end_hint,
                            bool existing, 
                            bool during_recovery 
                        ) { return _open_partition(n,  
                                    end_hint, existing,
                                    false, during_recovery); 
                          }
    partition_t *       _n_partition(partition_number_t n) const;
public:
    // exposed to partition_t
    partition_t *       curr_partition() const;

    /**
    * \brief Collect relevant logs to recover the given page.
    * \ingroup Single-Page-Recovery
    * \details
    * This method starts from the log record at EMLSN and follows
    * the page-log-chain to go backward in the log file until
    * it hits a page-img log from which we can reconstruct the
    * page or it reaches the current_lsn.
    * Defined in log_spr.cpp.
    * \NOTE This method returns an error if the user had truncated
    * the transaction logs required for the recovery.
    * @param[in] pid ID of the page to recover.
    * @param[in] current_lsn the LSN the page is currently at.
    * @param[in] emlsn the LSN up to which we should recover the page.
    * @param[out] log_copy_buffer the collected logs will be contiguously
    * copied in to this buffer in the \b reverse order of the log.
    * For example, the first entry would be the log record with EMLSN.
    * @param[in] buffer_size size of log_copy_buffer.
    * If we need a bigger buffer, we return an error.
    * @param[out] ordered_entries point to each log
    * record in log_copy_buffer in the order of the log.
    * This is easier to use for the purpose of applying them.
    * @pre current_lsn < emlsn
    */
    rc_t _collect_single_page_recovery_logs(
        const lpid_t& pid, const lsn_t &current_lsn, const lsn_t &emlsn,
        char* log_copy_buffer, size_t buffer_size,
        std::vector<logrec_t*> &ordered_entries,
        const bool valid_start_emlan = true);

    /**
    * \brief Apply the given logs to the given page.
    * \ingroup Single-Page-Recovery
    * Defined in log_spr.cpp.
    * @param[in, out] p the page to recover.
    * @param[in] ordered_entries the log records to apply
    * in the order of the log.
    * @pre p is already fixed with exclusive latch
    */
    rc_t _apply_single_page_recovery_logs(fixable_page_h &p,
        const std::vector<logrec_t*> &ordered_entries);

private:
    void                _sanity_check() const;
    partition_index_t   _get_index(partition_number_t)const; 

public:
protected:
}; // log_core

/*<std-footer incl-file-exclusion='LOG_H'>  -- do not edit anything below this line -- */

#endif          /*</std-footer>*/
