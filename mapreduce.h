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


namespace MAPREDUCE_NAMESPACE {

typedef struct kv{ // kv struct for use with reduction
    char key[MAX_WORD_LEN];
    int value;
} kv;

struct fileInfo{ // for storing fileInfo
    std::string fileName;
    long fileSize;
    fileInfo(const std::string &s, long fs){
        fileName = s;
        fileSize = fs;
    }
    ~fileInfo()=default;
};

template <class Key, class Value>
class MapReduce {
private:
    /* Work related variables */
    char * inputPath, *outputPath;      // 2 paths provided by user for input and output
    std::map<Key,Value> result;              // result map to store results
    std::queue<std::string> workqueue;            // queue to distribute work
    pthread_mutex_t countLock;          // mutex lock for obtaining and distributing work (inside slaves)
    // variables for directory
    size_t path_max;
    size_t name_max;

    /* MPI STUFF */
    int world_size, nrank, name_len;    // MPI Variables to keep track of current processor
    char * processor_name;              // name of processor (remember to deallocate)
    MPI_Comm comm;                      // communicator to use


    /* WORK COMMUNICATION FOR DISTRIBUTING TASKS */
    void distributeTask(char * dirPath); // to distribute task from master to workers --> assign slaves dir to file
    void distributeWork();
    void masterSendPath();              // explore path and send workers work
    void receiveWork();                 // slaves receive work from master
    void reduceCommunication();         // send final results to master

    /* Other utility functions */
    MPI_Datatype register_kv_type();    // function to register MPI type for sending
    void setup_machine_specifics();     // setup and initialize variables like path_max, name_max, etc.
public:
    KeyValue<Key, Value> *keyValue;     // keyValue class for mapping and reducing purposes... middleman style
    explicit MapReduce(MPI_Comm comm = MPI_COMM_WORLD, char* inputPath = NULL, char* outputPath= NULL);       // setup all the private variables and initializes MPI
    ~MapReduce();

    // the user will write the mapping function f (that only uses filename) to process file and emit (add) a kv pair.
    // Then the user will call map during mapping phase and mapreduce will take all files provided and run the function f on appropriate
    // machines (NOTE: Add ability for user to customize which node to run map on in the future)...
    void * engine(void*); // engine for mapper's threads
    void mapper( void (*f)(MapReduce<Key, Value> *, const char *));
    void reducer ( void (*f)(MapReduce<Key, Value> *) );        // user pass in output location?
    void emit(Key , Value);                                     // emit to kv class for mapper to send kv pairs
    void emit_final(Key,Value);                                 // emit to final map for output
    void sort_and_shuffle(bool (*compare)(Key, Key) = NULL);    // intermediate function user cal;
    void write_to_file();

    /* Queries */
    char * get_processor_name(){ return processor_name; }
    int get_world_size(){ return world_size;}
    int get_nrank(){ return nrank;}
    class Iterator;
    Iterator begin ()   ;
    Iterator end()      ;
    class Iterator {    // iterator class for user to loop through kv pairs with vector of values
    public:
        friend Iterator MapReduce<Key, Value>::begin();
        friend Iterator MapReduce<Key, Value>::end();
        ~Iterator();
        MapReduce<Key,Value>::Iterator& operator++();
        MapReduce<Key,Value>::Iterator operator++(int);
        bool operator == (const MapReduce<Key,Value>::Iterator& rhs) const;
        bool operator != (const MapReduce<Key,Value>::Iterator& rhs) const;
        std::pair<Key,std::vector<Value>>& operator* () const;      // dereferencing this iterator returns the collated kv pair
    private:
        typename std::map<Key, std::vector<Value>>::const_iterator current;
        explicit Iterator(KeyValue<Key,Value> *kv, int);
    };



}; // MapReduce class

} // MAPREDUCE_NAMESPACE
/* The main program looks something like
 *      MapReduce<mapFunction, reduceFunction> newTask;
 *      newTask.run(input, output); */


#endif //MAPREDUCECPP_MAPREDUCE_H
