#ifndef REPLICA_H
#define REPLICA_H

#include "command.h"
#include "../kits/shore_client.h"

class ShoreEnv;

class Replica : public Command
{
public:
	void setupOptions();
	void run();
protected:
	void initPrimary();
	void finishPrimary();
	void initSecondary();
	void finishSecondary();

	void loadOptions(sm_options& options,bool);
	void mkdirs(string);
	virtual void doWork(bool);
	void runBenchmark(bool);
	void createClients(bool);
	void forkClients(bool);
	void joinClients(bool);
	void ensureParentPathExists(string);
	void ensureEmptyPath(string);
	void archiveLog();
	void copyDevice();
private:
	int duration;
	int crashDelay;
	bool isPrimary;
	string p_logdir;	// Primary log directory
	ShoreEnv* p_shoreEnv;	// Primary shore environment
	string opt_dbfile;
	//int opt_queried_sf;
	//int opt_num_threads;
	//unsigned opt_duration;
	//bool opt_load;
	bool clientsForked;
	std::vector<base_client_t*> clients;
	string archdir;

	string s_logdir;	// Secondary log directory
	ShoreEnv* s_shoreEnv; 	// Secondary shore environment
	string s_dbfile;
};

#endif
