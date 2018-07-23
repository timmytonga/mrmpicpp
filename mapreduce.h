//
// Created by timmytonga on 7/18/18.
//

#ifndef MAPREDUCECPP_MAPREDUCE_H
#define MAPREDUCECPP_MAPREDUCE_H

#include "mpi.h"
#include <string>

#define MAXTHREADS 5 // this is for mappers to spawn threads
#define MAX_WORD_LEN 128 // only support up to 128 word len for counting... can do variable but inefficient...

namespace MAPREDUCE_NAMESPACE {
typedef struct kv{ // kv struct for use with reduction
    char key[MAX_WORD_LEN];
    int value;
} kv;

struct fileInfo{ // for storing fileInfo
    string fileName;
    long fileSize;
    fileInfo(string s, long fs){
        fileName = s;
        fileSize = fs;
    }
    ~fileInfo()=default;
};


template<class Mapper, class Reducer>
class MapReduce {
private:
    int world_size, nrank, name_len;    // MPI Variables to keep track of current processor
    map<string,int> result;             // result map to store results
    queue<string> workqueue;            // queue to distribute work
    char * processor_name;              // name of processor (remember to deallocate)
    pthread_mutex_t countLock;          // mutex lock for obtaining and distributing work (inside slaves)
    // variables for directory
    size_t path_max;
    size_t name_max;

    /* MAIN FUNCTION BY USER */
    void * mapFunction;             // this function should call the emit public function in order to collect KV pairs
    void * reduceFunction;          // this function takes KV pairs and call appro

    /* WORK COMMUNICATION FOR DISTRIBUTING TASKS */
    void distributeTask(char * dirPath); // to distribute task from master to workers --> assign slaves dir to file
    void masterSendPath(char * path, queue<string> &masterWQ); // explore path and send workers work
    void receiveWork();                 // slaves receive work from master


public:
    MapReduce(MPI_Comm comm);       // setup all the private variables and initializes MPI

    ~MapReduce();

    void run(String, String);
};

}
/* The main program looks something like
 *      MapReduce<mapFunction, reduceFunction> newTask;
 *      newTask.run(input, output); */


#endif //MAPREDUCECPP_MAPREDUCE_H
