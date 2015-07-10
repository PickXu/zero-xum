#ifndef EVENTLOG_H
#define EVENTLOG_H

#include "boost/date_time/gregorian/gregorian.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"

#define SM_SOURCE

#include "sm_base.h"
#include "logrec.h"

class sysevent_timer {
public:

    /*
     * Our timestamps are the number of miliseconds since
     * Jan 1, 2015
     */
    // Defined in logrec.cpp
    static boost::gregorian::date epoch;

    static const unsigned long MSEC_IN_DAY = 86400000;

    static unsigned long timestamp()
    {
        boost::posix_time::ptime t =
            boost::posix_time::microsec_clock::local_time();
        long days = (t.date() - epoch).days();
        long msec = t.time_of_day().total_milliseconds();

        return (unsigned long) days * MSEC_IN_DAY + msec;
    };

    static std::string timestamp_to_str()
    {
        // TODO implement
        return "";
    };
};

class sysevent {
public:
    static void log(logrec_t::kind_t kind);
    static void log_page_read(shpid_t shpid);
    static void log_page_write(shpid_t shpid, uint32_t cnt);
};

#endif