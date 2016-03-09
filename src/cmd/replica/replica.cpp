#include "replica.h"
#include "../kits/shore_env.h"

#include "../kits/tpcc/tpcc_env.h"
#include "../kits/tpcc/tpcc_client.h"
#include "../kits/util/stopwatch.h"

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

#include <zmq.hpp>
#include <fstream>
#include <iostream>

#include "protobuf/log_replication.pb.h"

#define Client tpcc::baseline_tpcc_client_t
#define Environment tpcc::ShoreTPCCEnv
#define EnvironmentPtr dynamic_cast<tpcc::ShoreTPCCEnv*>

int opt_queried_sf = 1;

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
	    std::ofstream myfile;

	    _subscriber.connect("tcp://"+host+":"+port);
	    _subscriber.setsockopt(ZMQ_SUBSCRIBE, "",0);
	    
	    std::cout << "Enter subscriber ... " << std::endl;

	    //NB: the log file name is hard code temporarily
	    myfile.open (logdir+"/log.1",std::ofstream::binary);

	    while(active) {
	            zmq::message_t logrec;

	            _subscriber.recv(&logrec);

		    replication::Replication rep;
		    rep.ParseFromArray(logrec.data(),logrec.size());
		    assert(rep.ByteSize() == logrec.size());

		    int file_offset = rep.fileoffset();
		    int data_size = rep.data_size();
		    const string& data = rep.log_data();

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
	mkdirs(p_logdir);
    } else {
    	options.set_string_option("sm_dbfile", s_dbfile);
	options.set_string_option("sm_logdir", s_logdir);
	options.set_string_option("sm_logport", "5557");
	options.set_bool_option("sm_restart_instant", true);
	mkdirs(s_logdir);
    }

    // ticker always turned on
    options.set_bool_option("sm_ticker_enable", true);
    //options.set_bool_option("sm_truncate_log", opt_truncateLog);
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

	int remaining = 30;	// Run 30 seconds
        while (remaining > 0) {
            remaining = ::sleep(remaining);
        }
}

void Replica::runBenchmark(bool isPrimary)
{
	if (isPrimary) {
		p_shoreEnv->reset_stats();
		stopwatch_t timer;
		TRACE(TRACE_ALWAYS, "[Primary] begin measurement\n");
		createClients(isPrimary);
		doWork(isPrimary);
		joinClients(isPrimary);
		double delay = timer.time();
		unsigned long miochs = 0;
                double usage = 0;
		TRACE(TRACE_ALWAYS, "[Primary] end measurement\n");
		p_shoreEnv->print_throughput(opt_queried_sf, true, 4, delay, miochs, usage);
	} else {
		s_shoreEnv->reset_stats();
		stopwatch_t timer;
		TRACE(TRACE_ALWAYS, "[Secondary] begin measurement\n");
		createClients(isPrimary);
		doWork(isPrimary);
		joinClients(isPrimary);
		double delay = timer.time();
		unsigned long miochs = 0;
                double usage = 0;
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

void Replica::copyDevice()
{
    std::ifstream source;
    std::ofstream target;

    //std::ifstream slog;
    //std::ofstream tlog;

    source.open(opt_dbfile,std::ios::binary);
    target.open(s_dbfile,std::ios::binary);

    target << source.rdbuf();

    //slog.open("log/log.1",std::ios::binary);
    //tlog.open("log_replica/log.1",std::ios::binary|std::ios::trunc);

    //tlog << slog.rdbuf();

    source.close();
    target.close();

    //slog.close();
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
	

void Replica::run()
{
    // Manually set some parameters
    optionValues.insert(std::make_pair("threads", po::variable_value(4,false)));

    if (isPrimary) {
    // Init the primary and publisher
    initPrimary();

    // Load the shore environment
    p_shoreEnv->load();
    cout << "Loading finished!" << endl;

    // Run the tpc-c benchmark
    runBenchmark(true);

    finishPrimary();
   
    } else {
    string host("localhost");
    string port("5556");

    // Start the subscriber to receive log records
    // from the primary
    Subscriber* sub = new Subscriber(s_logdir,host,port);
    sub->fork();

    ::sleep(2);

    // Wait till the primary finish its job
    while(getPrimary() > 0){
	cout << "waiting for primary to finish ..." << endl;
	::sleep(1);
    }

    sub->stop();

    // Copy Device File to Replica
    copyDevice();
    
    // Start the storage manager to initialize the 
    // the replica to the updated status
    initSecondary();

    runBenchmark(false);

    finishSecondary();
    sub->join();

    }
}
