#include "replica.h"
#include "../kits/shore_env.h"

#include "../kits/tpcc/tpcc_env.h"
#include "../kits/tpcc/tpcc_client.h"
#include "../kits/util/stopwatch.h"

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

#include <fstream>
#include <iostream>

#include "protobuf/log_replication.pb.h"

#define Client tpcc::baseline_tpcc_client_t
#define Environment tpcc::ShoreTPCCEnv
#define EnvironmentPtr dynamic_cast<tpcc::ShoreTPCCEnv*>
//#define HAVE_CPUMON

int opt_queried_sf = 1;

//procmonitor_t* _g_mon = ;
class CrashThread : public smthread_t
{
public:
    CrashThread(unsigned delay)
        : smthread_t(t_regular, "CrashThread"),
          delay(delay)
    {
    }

    virtual ~CrashThread() {}

    virtual void run()
    {

        ::sleep(delay);
        cerr << "Crash thread will now abort program" << endl;


	abort();
    }
private:
    unsigned delay;
};

class HeartBeatThread : public smthread_t 
{
public:
	HeartBeatThread()
		: smthread_t(t_regular, "HeartBeatThread"),
		active(true)
	{
	}

	virtual ~HeartBeatThread() {}

	virtual void run()
	{
		zmq::context_t context (1);
                zmq::socket_t socket (context, ZMQ_REP);
	        socket.bind ("tcp://*:6667");
		
		while (active) {
     		   	zmq::message_t request;

        		//  Wait for next request from client
        		socket.recv (&request);
        		cout << "SECONDARY-HEARTBEAT" << endl;

        		//  Send reply back to client
        		zmq::message_t reply (11);
        		memcpy (reply.data (), "PRIMARY-ACK", 11);
        		socket.send (reply);
    		}
	}

        virtual void stop()
	{
		active = false;
	}
private:
	bool active;
};

class MigrateThread : public smthread_t
{
public:
	MigrateThread(string path, string bwlimit, int interval)
			: smthread_t(t_regular, "MigrateThread"),
			path(path),
			bwlimit(bwlimit),
			interval(interval),
			active(true)
	{
	}
	
        virtual ~MigrateThread() {}

        virtual void run()
	{
		while(active) {
			system(("rsync -avz --bwlimit="+bwlimit+" xum@128.135.11.115:~/Documents/DB/zero/build/src/cmd/"+path+" "+path).c_str());
			::sleep(interval);
		}
	}

	virtual void stop()
	{
		active = false;
	}
private:
	string path;
	string bwlimit;
	int interval;	
	bool active;
};

class Subscriber : public smthread_t
{
public:
    Subscriber(string logdir, string host, string port)
    : smthread_t(t_regular, "Subscriber"),
      logdir(logdir),
      host(host),
      port(port),
      active(true)
    {
    }

    virtual ~Subscriber() {}
    virtual void run()
    {
	    zmq::context_t _context (1);
	    zmq::socket_t _subscriber (_context, ZMQ_SUB);
	    std::fstream myfile;

	    _subscriber.connect(("tcp://"+host+":"+port).c_str());
	    _subscriber.setsockopt(ZMQ_SUBSCRIBE, "",0);
	    
	    std::cout << "Enter subscriber ... " << std::endl;

            //NB: the log file name is hard code temporarily
            int curr_file = 1;
            myfile.open (logdir+"/log."+std::to_string(curr_file),std::ios::in | std::ios::out);
            if (!myfile.is_open()){
                myfile.open(logdir+"/log."+std::to_string(curr_file),std::ios::in | std::ios::out | std::ios::trunc);
            }

            while(active) {
                    zmq::message_t logrec;

                    _subscriber.recv(&logrec);

                    replication::Replication rep;
                    rep.ParseFromArray(logrec.data(),logrec.size());
                    assert(rep.ByteSize() == logrec.size());

                    int fileID = rep.fileid();
                    int file_offset = rep.fileoffset();
                    int data_size = rep.data_size();
                    const string& data = rep.log_data();

                    if (fileID != curr_file) {
                        curr_file = fileID;
                        myfile.close();
                        myfile.open (logdir+"/log."+std::to_string(curr_file),std::ios::in | std::ios::out);
                        if (!myfile.is_open()){
                            myfile.open(logdir+"/log."+std::to_string(curr_file),std::ios::in | std::ios::out | std::ios::trunc);
                        }
                    }

                    myfile.seekp(file_offset);
                    myfile.write(data.c_str(),data_size);
	    }
	
	    myfile.close();
    }

    virtual void stop() {
	    active = false;
    }

private:
    string logdir;
    string host;
    string port;
    bool active;
};


void Replica::setupOptions()
{
    // default value
    //long m = 274877906944L; // 256GB

    options.add_options()
        ("p_logdir,p", po::value<string>(&p_logdir)->required(),
            "Log directory of the primary replica")
	("s_logdir,s", po::value<string>(&s_logdir)->required(),
	    "Log directory of the secondary replica")
	("p_db_file", po::value<string>(&opt_dbfile)->default_value("db"),
	    "Primary DB file")
	("s_db_file", po::value<string>(&s_dbfile)->default_value("sdb"),
	    "Secondary DB file")
	("isPrimary", po::value<bool>(&isPrimary)->required(),
	    "Indicate that whether it's a primary or secondary replica")
	("duration", po::value<int>(&duration)->required(),
	    "The duration of the benchmark")
	("crashDelay", po::value<int>(&crashDelay),
	    "The time elapsed before crash")
	("archdir,a", po::value<string>(&archdir)->default_value("archive"),
	    "Directory in which to store the log archive")
    ;
    setupSMOptions();
}

void Replica::mkdirs(string path)
{
    // if directory does not exist, create it
    fs::path fspath(path);
    if (!fs::exists(fspath)) {
        fs::create_directories(fspath);
    }
    else {
        if (!fs::is_directory(fspath)) {
            throw runtime_error("Provided path is not a directory!");
        }
    }
}

void Replica::loadOptions(sm_options& options, bool isPrimary)
{
    //options.set_bool_option("sm_truncate", opt_load);
    if (isPrimary) {
    	options.set_string_option("sm_logdir", p_logdir);
	options.set_string_option("sm_logport","5556");
	options.set_bool_option("sm_format",true);
	mkdirs(p_logdir);
#ifdef LOG_ARCHIVE_RECOVERY
	if (!archdir.empty()) {
		options.set_string_option("sm_archdir", archdir);
	        options.set_bool_option("sm_archiver_eager", true);
		options.set_bool_option("sm_archiving", true);
        	mkdirs(archdir);
    	}
#endif
    } else {
    	options.set_string_option("sm_dbfile", s_dbfile);
	options.set_string_option("sm_logdir", s_logdir);
	options.set_string_option("sm_logport", "5557");
	options.set_bool_option("sm_restart_instant", true);
#ifdef LOG_ARCHIVE_RECOVERY
	options.set_bool_option("sm_restore_instant", true);
	options.set_bool_option("sm_restore_sched_singlepass", true);
	options.set_bool_option("sm_restore_sched_ondemand",true);
#endif
	mkdirs(s_logdir);
    }

    // ticker always turned on
    options.set_bool_option("sm_ticker_enable", true);
    options.set_bool_option("sm_truncate_log", false);
}


void Replica::ensureEmptyPath(string path)
{
	fs::path fspath(path);
	if(!fs::exists(path)) {
		return;
	}

	if (!fs::is_empty(fspath)) {
		if (fs::is_directory(fspath)) {
			fs::directory_iterator end, it(fspath);
			while(it != end) {
				fs::remove_all(it->path());
				it++;
			}
		}
	}

}

void Replica::ensureParentPathExists(string path)
{
	fs::path fspath(path);
	fspath.remove_filename();
	string parent = fspath.string();
	if (parent.empty()) {return ;}

	mkdirs(parent);
}

void Replica::initPrimary()
{
	p_shoreEnv = new Environment(optionValues);

	loadOptions(p_shoreEnv->get_opts(), true);

	p_shoreEnv->set_sf(opt_queried_sf);
	p_shoreEnv->set_qf(opt_queried_sf);
	p_shoreEnv->set_loaders(4);
	p_shoreEnv->set_logport("5556");

	p_shoreEnv->init();
	p_shoreEnv->set_clobber(true);

	ensureEmptyPath(p_logdir);
	ensureParentPathExists(opt_dbfile);
	p_shoreEnv->set_device(opt_dbfile);

	p_shoreEnv->start();
}

void Replica::finishPrimary()
{
	p_shoreEnv->close();
}

void Replica::initSecondary()
{
    s_shoreEnv = new Environment(optionValues);

    loadOptions(s_shoreEnv->get_opts(),false);

    s_shoreEnv->set_sf(opt_queried_sf);
    s_shoreEnv->set_qf(opt_queried_sf);
    s_shoreEnv->set_loaders(4);
    s_shoreEnv->set_logport("5557");

    s_shoreEnv->init();
    s_shoreEnv->set_clobber(false);
    ensureParentPathExists(s_dbfile);
    s_shoreEnv->set_device(s_dbfile);

    s_shoreEnv->start();
}

void Replica::finishSecondary()
{
    s_shoreEnv->close();
}

void Replica::createClients(bool isPrimary)
{
	int current_prs_id = -1;
	int wh_id = 0;

	int trxsPerThread = 10000/4;
	for(int i=0;i<4;i++) {
		wh_id = (i%(int)opt_queried_sf)+1;

		Client* client = new Client("client-"+std::to_string(i), i,
				EnvironmentPtr((isPrimary)?p_shoreEnv:s_shoreEnv),
				MT_TIME_DUR, 1,
				trxsPerThread,
				current_prs_id,
				wh_id, opt_queried_sf);
		clients.push_back(client);
	}
}

void Replica::forkClients(bool isPrimary)
{
	for (size_t i=0;i<clients.size();i++)
		clients[i]->fork();

	clientsForked = true;
	if (isPrimary)
		p_shoreEnv->set_measure(MST_MEASURE);
	else 
		s_shoreEnv->set_measure(MST_MEASURE);
}

void Replica::joinClients(bool isPrimary)
{
	if (isPrimary)
		p_shoreEnv->set_measure(MST_DONE);
	else 
		s_shoreEnv->set_measure(MST_DONE);

	if (clientsForked) {
	    for (size_t i = 0; i < clients.size(); i++) {
	        clients[i]->join();
	        if (clients[i]->rv()) {
	            throw runtime_error("Client thread reported error");
	        }
	        delete (clients[i]);
	    }
	    clientsForked = false;
	    clients.clear();
	}	
}

void Replica::doWork(bool isPrimary)
{
	forkClients(isPrimary);

	int remaining = duration;
        while (remaining > 0) {
            remaining = ::sleep(remaining);
        }
}

void Replica::runBenchmark(bool isPrimary)
{
#ifdef HAVE_CPUMON
	_g_mon->cntr_reset();
#endif

	if (isPrimary) {
		p_shoreEnv->reset_stats();
		stopwatch_t timer;
		TRACE(TRACE_ALWAYS, "[Primary] begin measurement\n");
		createClients(isPrimary);
		doWork(isPrimary);
		joinClients(isPrimary);
		double delay = timer.time();
#ifdef HAVE_CPUMON
    _g_mon->cntr_pause();
    unsigned long miochs = _g_mon->iochars()/MILLION;
    double usage = _g_mon->get_avg_usage(true);
#else
    unsigned long miochs = 0;
    double usage = 0;
#endif
		TRACE(TRACE_ALWAYS, "[Primary] end measurement\n");
		p_shoreEnv->print_throughput(opt_queried_sf, true, 4, delay, miochs, usage);
	} else {
#ifdef LOG_ARCHIVE_RECOVERY
		// Set the volume as failed
		vol_t* vol = smlevel_0::vol;
		w_assert0(vol);
		vol->mark_failed(false);
#endif

		s_shoreEnv->reset_stats();
		stopwatch_t timer;
		TRACE(TRACE_ALWAYS, "[Secondary] begin measurement\n");
		createClients(isPrimary);
		doWork(isPrimary);
		joinClients(isPrimary);
		double delay = timer.time();
#ifdef HAVE_CPUMON
    _g_mon->cntr_pause();
    unsigned long miochs = _g_mon->iochars()/MILLION;
    double usage = _g_mon->get_avg_usage(true);
#else
    unsigned long miochs = 0;
    double usage = 0;
#endif
		TRACE(TRACE_ALWAYS, "[Secondary] end measurement\n");
		s_shoreEnv->print_throughput(opt_queried_sf, true, 4, delay, miochs, usage);
	}
}

void Replica::archiveLog()
{
    // archive whole log
    smlevel_0::logArchiver->activate(smlevel_0::log->curr_lsn(), true);
    while (smlevel_0::logArchiver->getNextConsumedLSN() < smlevel_0::log->curr_lsn()) {
        usleep(1000);
    }
    smlevel_0::logArchiver->shutdown();
    smlevel_0::logArchiver->join();
}

void Replica::copyDevice(string bwlimit)
{
/*
    // COPY LOCAL FILE
    std::ifstream source;
    std::ofstream target;


    source.open(opt_dbfile,std::ios::binary);
    target.open(s_dbfile,std::ios::binary);

    target << source.rdbuf();

    source.close();
    target.close();
*/

    system(("rsync -avz --bwlimit="+bwlimit+" xum@128.135.11.115:~/Documents/DB/zero/build/src/cmd/db sdb").c_str());
    //tlog.close();
        
}

int getPrimary()
{
    int pid = -1;

    // Open the /proc directory
    DIR *dp = opendir("/proc");
    if (dp != NULL)
    {
        // Enumerate all entries in directory until process found
        struct dirent *dirp;
        while (pid < 0 && (dirp = readdir(dp)))
        {
            // Skip non-numeric entries
            int id = atoi(dirp->d_name);
            if (id > 0)
            {
                // Read contents of virtual /proc/{pid}/cmdline file
                string cmdPath = string("/proc/") + dirp->d_name + "/cmdline";
                ifstream cmdFile(cmdPath.c_str());
                string cmdLine;
                getline(cmdFile, cmdLine);
                if (!cmdLine.empty())
                {
                    // Compare against requested process name
                    if (cmdLine.find("isPrimary") != string::npos && cmdLine.find("true") != string::npos)
                        pid = id;
                }
            }
        }
    }

    closedir(dp);

    return pid;
}

void signalHandler(int signum)
{
	cout << "Interrupt signal (" << signum << ") received.\n";
	zmq::context_t _context(1);
	zmq::socket_t _ps(_context, ZMQ_PUB);
        _ps.bind("tcp://*:6667");
        _ps.bind("ipc://repl-control.ipc");

	string msg_content("CTRL-MSG: FIN");
        zmq::message_t ctrl_msg((void*)msg_content.data(),13, NULL);
        _ps.send(ctrl_msg);

	return;
}

void Replica::run()
{
    // Manually set some parameters
    optionValues.insert(std::make_pair("threads", po::variable_value(4,false)));

    if (isPrimary) {
    //int linger=0;
    //_ps.setsockopt(ZMQ_LINGER,&linger,sizeof(int));

    // Start primary heartbeat thread
    HeartBeatThread* hbt = new HeartBeatThread();
    hbt->fork();
    
#ifdef LOG_ARCHIVE_RECOVERY
    // Start the log achive migration thread if archive is set
    MigrateThread* mt;
    if (!archdir.empty()){
	    mt = new MigrateThread(archdir, "50000", 5);
	    mt->fork();
    }
#endif

    // Init the primary and publisher
    initPrimary();

    // Load the shore environment
    p_shoreEnv->load();
    cout << "Loading finished!" << endl;

    // Trigger crash after 20 seconds
    CrashThread* t = new CrashThread(crashDelay);
    t->fork();

    // Run the tpc-c benchmark
    runBenchmark(true);

    finishPrimary();
    
#ifdef LOG_ARCHIVE_RECOVERY
    if (mt) mt->stop();
#endif
    hbt->stop();
   
    } else {
    string host("128.135.11.115");
    string port1("5556");	// Port for log replication
    string port2("6667");	// Port for out-of-band control
    bool active = true;

    // Start the subscriber to receive log records
    // from the primary
    Subscriber* sub = new Subscriber(s_logdir,host,port1);
    sub->fork();

    // Start the dbfile migration thread
    MigrateThread* mt = new MigrateThread("db", "5000", 5);
    mt->fork();

    zmq::context_t context (1);
    zmq::socket_t socket (context, ZMQ_REQ);
    std::cout << "Connecting to primary…" << std::endl;
    socket.connect (("tcp://"+host+":"+port2).c_str());

    zmq_pollitem_t item;
    item.socket = socket;
    item.events = ZMQ_POLLIN;
    zmq::message_t request (19);
    memcpy (request.data (), "SECONDARY-HEARTBEAT", 19);

    //  send heartbeat requests every 5 second
    while(active) {
	::sleep(5);
	cout << "Sending heartbeat …" << endl;

	socket.send (request);

        zmq::message_t reply;
	// Wait for ACK with 10 seconds timeout
	zmq::poll(&item,1,10);
        //  Get the reply.
	if (item.revents & ZMQ_POLLIN) {
        	socket.recv (&reply);
        	cout << "Received ACK from Primary" << endl;
        } else {
		// Timeout exception
		active = false;
	}
    }


    sub->stop();
    mt->stop();

    // Copy Device File to Replica
    stopwatch_t timer;
    //Make sure the two dbfiles are identical
    copyDevice("10000");
    double delay = timer.time();
    cout << "Copy DB Latency: " << delay << endl;
    
    // Start the storage manager to initialize the 
    // the replica to the updated status
    initSecondary();

    runBenchmark(false);

    finishSecondary();

    }
}
