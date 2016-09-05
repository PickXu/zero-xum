#include "logpagestats.h"

class LogPageStatsHandler : public Handler {
public:
    size_t pageCount;
    size_t logrecs;
    size_t volume;
    tid_t  tid;    
    const char* type;
    PageID currentPage;

    LogPageStatsHandler() : pageCount(0), logrecs(0), volume(0),
        currentPage(0)
    {}

    virtual void invoke(logrec_t& r)
    {
        PageID pid = r.pid();
 
        if (pid != currentPage) {
            dumpCurrent();

	    if(currentPage > pid)
		pageCount = currentPage;

            logrecs = 0;
            volume = 0;
            currentPage = pid;
	    tid = r.tid();
	    type = r.type_str();
        }


        logrecs++;
        volume += r.length();
    }

    void dumpCurrent() {
        if (logrecs == 0) { return; }

        cout << "pid=" << currentPage
            << " count=" << logrecs
            << " tid=" << tid.as_int64()
	    << " type=" << type
            << endl;
    }

    virtual void finalize()
    {
        dumpCurrent();
        pageCount++;
        cout << "TOTAL_PAGES=" << pageCount << endl;
    };
};

void LogPageStats::setupOptions()
{
    LogScannerCommand::setupOptions();
}

void LogPageStats::run()
{
    LogPageStatsHandler h;
    BaseScanner* s = getScanner();

    s->add_handler(&h);
    s->fork();
    s->join();

    delete s;
}

