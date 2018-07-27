// Created by timmytonga on 5/21/18.
//      LAST MODIFIED: JUL 6
//
#define DEBUG // for debugging purposes

#include <mpi.h>
#include "errors.h"     // errors and debugging
/* Data structure and io*/
#include <iostream>
#include <fstream>      // file io
#include <string>
#include <queue>
#include <map>
#include <vector>
/* Dir and file */
#include <unistd.h>
#include <sys/stat.h>   // for stat(path, filestat)
#include <dirent.h>     // DIR, opendir, and  dirent
#include <cstddef>      // for offset macro
/* Multi-threading */
#include <pthread.h>

#define MAXTHREADS 5 // this is for mappers to spawn threads
#define MAX_WORD_LEN 128 // only support up to 128 word len for counting... can do variable but inefficient...

using namespace std;    // boom

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

/* global multithreading vars */
pthread_mutex_t countLock;
// maximum size for path and name
size_t path_max;
size_t name_max;
char processor_name[MPI_MAX_PROCESSOR_NAME];
int world_size, nrank, name_len;

map<string, int> result;
queue<string> workqueue;

/* In construction: Distribute task evenly with scatterv */
void distributeTask(char * dirPath) { // explore path and send workers work
    // we have the path of dir. We want to see how many files there are and their sizes and
    //  then break up the work as evenly as possible among processes. We will first explore the dir and
    //  grab all files. Then we will figure out how to split up the files evenly among tasks so that each
    //  has roughly the same amount of data to work with. Then within each tasks, we will further split up the files
    //  for multithreading.

    /* Some info about work */
    long totalSize = 0;
    int numFiles, filesPerTask, remainder, sum=0;

    vector<fileInfo> workVector; // datastructure we use to store fileInfo for further processing
    /* First explore the directory and obtain information about files */
    if (nrank == 0) {
        workVector.reserve(10); // reserve 10?
        struct stat filestat;   // file stat
        int status;             // for error checking
        status = stat(dirPath, &filestat);
        if (status != 0) fprintf(stderr, "Error stat opening %s: %s\n", dirPath, strerror(errno));
        if (S_ISDIR(filestat.st_mode)) { // if it's dir then we obtain all the files along with their sizes
            DIR *directory;     // struct to explore directory
            struct dirent *result;      // result for direntry
            directory = opendir(dirPath);
            if (directory == NULL) {
                fprintf(stderr, "Unable to open directory %s: %s\n", dirPath, strerror(errno));
                MPI_Abort(MPI_COMM_WORLD, errno);
            }
            // now that we have a working directory, we obtain the files and its sizes
            while (1) {
                errno = 0;
                result = readdir(directory);
                if (result == NULL) {
                    if (errno != 0) fprintf(stderr, "An error occurred during readdir: %s\n ", strerror(errno));
                    break; // we have reached the end of directory (or error if printed above)
                }
                if (result->d_type != DT_REG) { // if it's not a regular file
                    continue; // ignore??? or error?
                }
                // ignore . and ..
                if (strcmp(result->d_name, ".") == 0) continue;
                if (strcmp(result->d_name, "..") == 0) continue;
                string newPath(dirPath); // this is our new path to our file
                newPath += "/";
                newPath += result->d_name;  // location to entry
                status = stat(newPath.c_str(), &filestat);
                if (status != 0) {
                    fprintf(stderr, "Error stat opening %s: %s\n", dirPath, strerror(errno));
                    //continue; // should we skip????
                }
                long fileSize = filestat.st_size; // the fileSize in bytes
                totalSize += fileSize; // this totalSize helps calculate the load each task receives
                workVector.push_back(fileInfo(newPath, fileSize));
            }
        } else { // only take directory as inputs
            fprintf(stderr, "ERROR: Input directory only. Received: %s \n", dirPath);
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
        numFiles = workVector.size();

    } // end if (nrank == 0)

    // let other nodes know of the total size
    MPI_Bcast(&filesPerTask, 1, MPI_INT, 0, MPI_COMM_WORLD);
    /* Now we have a vector of file paths and sizes (for 0). We will try to divide up the files as evenly as possible among nodes
     *  Right now, we assume the input files to be roughly the same size (by splitting)*/
    filesPerTask = numFiles/world_size;
    remainder = numFiles%world_size;

    int * sendcounts = new int[world_size];
    int * displacement = new int[world_size];
    // calculate size and displacements for scattering
    for (int i = 0; i<world_size; i++){
        sendcounts[i] = filesPerTask;
        if (remainder > 0){
            sendcounts[i]++;
            remainder--;
        }
        displacement[i]=sum;
        sum+=sendcounts[i];
    }

    //MPI_Scatterv(&workVector[0]); // this is the array of size numFiles
}

void masterSendPath(char * path, queue<string> &masterWQ){ // explore path and send workers work
    struct stat filestat;
    int status;

    status = stat(path, &filestat);
    // only process directory and obtain files from dir
    if (S_ISDIR(filestat.st_mode)){
        DIR * directory;
        struct dirent* result;
        directory = opendir(path);
        if (directory == NULL){
            fprintf(stderr, "Unable to open directory\n");
            return;
        }
        int dest = 0;  // for sending 1 by 1
        while(1){
            result = readdir(directory);
            if (result == NULL) {
                break; // end of dir
            }
            // skip . and ..
            if (strcmp (result->d_name, ".") == 0) continue;
            if (strcmp (result->d_name, "..") == 0) continue;

            char newpath[path_max];
            strcpy(newpath, path);
            strcat(newpath, "/");
            strcat(newpath, result->d_name);
            // now we send the newpath
            if (dest == 0) {
                DPRINTF(("Sending path %s/%s to %d\n", path, result->d_name, dest));
                masterWQ.push(newpath);
            }
            else{
                DPRINTF(("Sending path %s/%s to %d\n", path, result->d_name, dest));
                MPI_Send(newpath, path_max, MPI_CHAR, dest, 0, MPI_COMM_WORLD);
            }

            /* OPTIMIZE BY SENDING A BUNCH OF PATHS TOGETHER -- PARTITION FIRST
             * AND THEN SEND ONCE TO EACH NODE
             *  --> WANT TO MINIMIZE COMMUNICATIONS (THE NUMBER OF SEND/RECV CALLS */
            dest = (dest+1)%(world_size); // update
        }
        DPRINTF(("MASTER FINISHING...CLOSING DIR....\n"));
        closedir(directory);
    }
    else{
        fprintf(stderr, "ERROR: Input directory only.\n");
        MPI_Finalize();
        exit(1);
    }

    //MPI_Bcast( d, 5, MPI_CHAR, 0, MPI_COMM_WORLD);
    for (int i = 1; i < world_size; i++) // signal done
        MPI_Send("",1, MPI_CHAR, i, 1, MPI_COMM_WORLD); // tag 1 for done while tag 0 for work
}

void receiveWork(queue<string>  &workqueue){
    int ierr;
    MPI_Status status;
    char buf[path_max];
    while(1){
        /*MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_CHAR, &path_len);
        if (path_len == 1) break; // signal (empty str) */
        ierr = MPI_Recv(&buf, path_max, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        if (ierr != 0) fprintf(stderr, "Error: MPI Receive in receive work: %s \n", strerror(ierr));
        //DPRINTF(("Processor %s, nrank %d: waiting to receive...\n", processor_name, nrank));
        if (status.MPI_TAG == 1){
            break;
        }
        else{
            workqueue.push(string(buf));
            DPRINTF(("Processor %s, rank %d: Just pushed %s to front of queue\n",processor_name, nrank, workqueue.front().c_str()));
        }
    }
}

void wordcount(const char * path){
    struct stat filestat;
    int status;
    status = stat(path, &filestat);
    if (S_ISLNK(filestat.st_mode))
        printf("%s is a link. Skipping...\n", path);
    if (S_ISDIR(filestat.st_mode)){
        // to do  --> master should only send regular files to slaves...
    }
    else if (S_ISREG(filestat.st_mode)){
        // if this is a regular file, we want to perform word count on this file,
        //      get the word count in sorted order
        //      then send the words with its data to master for big reduction and output
        //   Make faster: if file size big, split and multi thread the wordcount
        map<string,int> temp;
        ifstream file;
        file.open(path); // open the file
        if (!file.is_open()){
            fprintf(stderr, "ERROR (Processor %s rank %d): Cannot open file %s\n ", processor_name, nrank, path);
            return;
        }
        string word;
        while(file >> word)  // read words in file
            temp[word]++; // increment count in result
        file.close();
        pthread_mutex_lock(&countLock);
        for (auto kv: temp)
            result[kv.first] += kv.second;
        pthread_mutex_unlock(&countLock);

    }
}

void * engine(void * data) {
    string temp;
    while (1){ // while not empty
        pthread_mutex_lock(&countLock);
        if (workqueue.empty()){
            pthread_mutex_unlock(&countLock);
            break;
        }
        temp = workqueue.front();
        workqueue.pop();
        pthread_mutex_unlock(&countLock);
        wordcount(temp.c_str());
    }
}
void slaveMap(queue<string> workqueue){
    /* We use threads to further parallize */
    DPRINTF((" @@IN MAP: Processor %s, nrank %d: workqueue %s\n ", processor_name, nrank, workqueue.front().c_str()));
    int status, numthreads;
    int worksize = workqueue.size();
    status = pthread_mutex_init(&countLock, nullptr);
    if (status != 0) err_abort(status, "Initialize countLock in start \n");
    numthreads = worksize < MAXTHREADS ? worksize : MAXTHREADS;
    pthread_t threads[numthreads];
    for (int i = 0; i < numthreads; i++){
        status = pthread_create(&threads[i], NULL, engine, NULL);
        if (status!= 0) err_abort(status, "Create worker");
    }
    for (int i = 0; i<numthreads; i++){
        status = pthread_join(threads[i],NULL);
        if (status != 0) err_abort(status, "Joining workers");
    }
}

MPI_Datatype register_kv_type(){
    const int       structlen = 2;
    int             lengths[structlen] = { MAX_WORD_LEN , 1};
    MPI_Aint        offsets[structlen] = {offsetof(kv, key), offsetof(kv, value)};
    MPI_Datatype    types[structlen] = {MPI_CHAR, MPI_INT};

    MPI_Datatype kvtype;
    MPI_Type_struct(structlen, lengths, offsets, types, &kvtype);
    MPI_Type_commit(&kvtype);
    return kvtype;
}

void reduce(map<string, int> &result){
    // if master then receive data sets and add up result
    // else we send our maps to master
    DPRINTF(("Rank %d: I am in Reduce\n", nrank));
    MPI_Datatype kvtype = register_kv_type(); // register the kv type... remember to free after finish
    if (nrank == 0){ // master... receive from slaves and add them up modifying result
        MPI_Status status, status2;
        /* FIRST COLLECT PACKAGES FROM SLAVES ...  */
        kv *  collect[world_size-1];     // array to store results from slaves
        int collectsize[world_size-1]; // to remember the size of the kv from
        int packagesize;                // size of receiving package (the kv pairs that slaves send below)
        for (int i = 0; i < world_size-1; i++){ // -1 because we are not doing anything
            MPI_Probe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status); // PROBE to get package size
            MPI_Get_count(&status, kvtype, &packagesize);
            collect[i]      = new kv[packagesize]; // remember to free after done counting and adding to result !!!!
            collectsize[i]  = packagesize;
            MPI_Recv(collect[i], packagesize, kvtype, status.MPI_SOURCE, 0,MPI_COMM_WORLD, &status2);
        }
        // now collect has a bunch of kv arrays. we add each on result (can parallelize with threads)
        for (int i = 0; i < world_size-1; i++){
            for (int j = 0; j < collectsize[i]; j++){
                kv temp = collect[i][j];
                result[temp.key] += temp.value;
            }
            // now we are done with that collect
            delete [] collect[i];
        }
    }
    else{ // send pairs of data --> master gather these pairs and process
        /* PACKAGE THE MAP AND THEN SEND IT TO MASTER */
        long size = result.size();
        kv  * package = new kv[size];
        int i = 0;
        /* THIS IS THE REDUCTION PART */
        for (auto resultkv : result){
            strcpy(package[i].key, resultkv.first.c_str());
            package[i++].value = resultkv.second;
        }
        MPI_Send(package, size, kvtype, 0, 0, MPI_COMM_WORLD);
        delete [] package;
    }
    // FREE TYPE
    MPI_Type_free(&kvtype);
}

void writeToFile(char* filename, const map<string, int> & result){
    /* given a map output, create a file with filename and write wordcount */
    DPRINTF(("Rank %d: I'm in write file\n", nrank));
    ofstream outfile(filename);
    // since map is already ordered, we just iterate through it and write to file
    for (auto i : result){
        outfile << i.first <<  "\t" << i.second << endl;
    }
    outfile.close();
}

void start(char * filepath){
    /* Setup variables (pathmax, namemax) and thread pools
     *  First get path length and name length */
    path_max = (size_t)pathconf(filepath, _PC_PATH_MAX);
    if (path_max == -1){
        if (errno == 0) path_max = 1024;
        else errno_abort("Unable to get PATH_MAX");
    }
    name_max = (size_t)pathconf(filepath, _PC_NAME_MAX);
    if (name_max == -1){
        if (errno == 0) name_max = 256;
        else errno_abort("Unable to get NAME_MAX");
    }
    // for null char
    path_max++;
    name_max++;
}


int main(int argc, char ** argv){
    /* Send argv to all */
    MPI_Init(&argc, &argv); // argv will be original link
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &nrank);
    MPI_Get_processor_name(processor_name, &name_len);


    printf("Hello this is processor %s, nrank %d.\n", processor_name, nrank);
    if (argc != 3){
        fprintf(stderr, "Usage: ... ./wordcount <src_dir> <output_dest>\n argc = %d\n", argc);
        MPI_Abort(MPI_COMM_WORLD, 911);
        exit(1);
    }
    start(argv[1]);
    /* FIRST DISTRIBUTE WORK */
    if (nrank == 0){
        /* Setup job... open argv[1] (original dir), obtain paths of containing files */
        printf("Processor %s, nrank %d: Starting with %s\n", processor_name, nrank, argv[1]);
        masterSendPath(argv[1], workqueue);
        // receive til finish
    }
    else{ // slave
        receiveWork(workqueue);
    }
    // NOW EACH NODE HAS A WORKQUEUE... WE WILL MAP THEN REDUCE
    slaveMap(workqueue);
    // wait done mapping... do i need to wait?
    DPRINTF(("RANK %d: Finished Mapping\n ============ " , nrank));
    //MPI_Barrier(MPI_COMM_WORLD);
    reduce(result); // all process send result to master for reducing
    // finish by writing to file
    MPI_Barrier(MPI_COMM_WORLD);

    if (nrank == 0)
        writeToFile(argv[2], result);
    MPI_Barrier(MPI_COMM_WORLD);

    DPRINTF(("Rank %d: Finished!===============================\n",nrank));
    MPI_Finalize();
}



