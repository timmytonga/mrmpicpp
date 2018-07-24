//
// Created by timmytonga on 7/18/18.
//

#ifndef MAPREDUCECPP_MAPREDUCE_H
#define MAPREDUCECPP_MAPREDUCE_H

#include "mpi.h"
#include "keyvalue.h"

#include <string>
#include <iostream>
#include <map>
#include <queue>

#define MAXTHREADS 5 // this is for mappers to spawn threads
#define MAX_WORD_LEN 128 // only support up to 128 word len for counting... can do variable but inefficient...

using namespace std;

namespace MAPREDUCE_NAMESPACE {
typedef struct kv{ // kv struct for use with reduction
    char key[MAX_WORD_LEN];
    int value;
} kv;

struct fileInfo{ // for storing fileInfo
    string fileName;
    long fileSize;
    fileInfo(const string &s, long fs){
        fileName = s;
        fileSize = fs;
    }
    ~fileInfo()=default;
};

template <class Key, class Value>
class MapReduce {
private:
    /* Work related variables */
    KeyValue<Key, Value> *keyValue;     // keyValue class for mapping and reducing purposes... middleman style
    char * inputPath, *outputPath;      // 2 paths provided by user for input and output
    map<Key,Value> result;              // result map to store results (Replace with a KV class)
    queue<string> workqueue;            // queue to distribute work
    pthread_mutex_t countLock;          // mutex lock for obtaining and distributing work (inside slaves)
    // variables for directory
    size_t path_max;
    size_t name_max;

    /* MPI STUFF */
    int world_size, nrank, name_len;    // MPI Variables to keep track of current processor
    char * processor_name;              // name of processor (remember to deallocate)
    MPI_Comm comm;                    // communicator to use

    /* MAIN FUNCTION BY USER */
    void * mapFunction;             // this function should call the emit public function in order to collect KV pairs
    void * reduceFunction;          // this function takes KV pairs and call appro

    /* WORK COMMUNICATION FOR DISTRIBUTING TASKS */
    void distributeTask(char * dirPath); // to distribute task from master to workers --> assign slaves dir to file
    void masterSendPath(char * path, queue<string> &masterWQ); // explore path and send workers work
    void receiveWork();                 // slaves receive work from master


    /* Other utility functions */
    MPI_Datatype register_kv_type();    // function to register MPI type for sending
    void setup_machine_specifics();     // setup and initialize variables like path_max, name_max, etc.
public:
    MapReduce(MPI_Comm comm, char* inputPath, char* outputPath);       // setup all the private variables and initializes MPI
    ~MapReduce();

    // the user will write the mapping function f (that only uses filename) to process file and emit (add) a kv pair.
    // Then the user will call map during mapping phase and mapreduce will take all files provided and run the function f on appropriate
    // machines (NOTE: Add ability for user to customize which node to run map on in the future)...
    void mapper( void (*f)(char*));
    void reducer ( void (*f)(char*));    // user pass in output location?
    void emit(Key , Value);            // emit to kv class for reducing
    void sort_and_shuffle(bool (*compare)(Key, Key) = NULL);                     // intermediate function user cal;

};

}
/* The main program looks something like
 *      MapReduce<mapFunction, reduceFunction> newTask;
 *      newTask.run(input, output); */


#endif //MAPREDUCECPP_MAPREDUCE_H
