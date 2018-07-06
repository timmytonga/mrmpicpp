//
// Created by timmytonga on 5/21/18.
//      LAST MODIFIED: JUL 6
//
#define DEBUG // for debugging purposes

#include <mpi/mpi.h>
#include "errors.h"
/* Data structure and io*/
#include <iostream>
#include <fstream>
#include <string>
#include <queue>
#include <map>
/* Dir and file */
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <cstddef>      // for offset macro

#define NUMTHREADS 20 // this is for mappers to spawn threads
#define MAX_WORD_LEN 128 // only support up to 128 word len for counting... can do variable but inefficient...

using namespace std;

typedef struct kv{
    char key[MAX_WORD_LEN];
    int value;
} kv;

// maximum size for path and name
size_t path_max;
size_t name_max;
char processor_name[MPI_MAX_PROCESSOR_NAME];
int world_size, rank, name_len;

void start(char * filepath){
    /* First get path length and name length */
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

void masterSendPath(char * path, queue<char*> &masterWQ){ // explore path and send workers work
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
                DPRINTF(("==================== MASTER: END OF DIR ================ \n"));
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
            if (dest == 0)
                masterWQ.push(newpath);
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

void receiveWork(queue<char*>  &workqueue){
    int ierr;
    MPI_Status status;
    char buf[path_max];
    while(1){
        /*MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_CHAR, &path_len);
        if (path_len == 1) break; // signal (empty str) */
        ierr = MPI_Recv(&buf, path_max, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        DPRINTF(("Processor %s, rank %d: waiting to receive...\n", processor_name, rank));
        if (status.MPI_TAG == 1){
            break;
        }
        else{
            DPRINTF(("Processor %s, rank %d: Received path %s\n", processor_name, rank, buf));
            workqueue.push(buf);
        }
    }
}

void wordcount(char * path, map<string,int> & result){
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
        ifstream file;
        file.open(path); // open the file
        if (!file.is_open())
            cerr << "ERROR: Cannot open file " << path << endl;
        string word;
        while(file >> word)  // read words in file
            result[word]++; // increment count in result
        file.close();
    }
}

void slaveMap(map<string, int> &result, queue<char*> workqueue){
    while (!workqueue.empty()){ // while not empty
        wordcount(workqueue.front(), result);
        workqueue.pop();
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
    MPI_Datatype kvtype = register_kv_type(); // register the kv type... remember to free after finish
    if (rank == 0){ // master... receive from slaves and add them up modifying result
        MPI_Status status, status2;
        /* FIRST COLLECT PACKAGES FROM SLAVES  */
        kv *    collect[world_size-1];     // array to store results from slaves
        int     collectsize[world_size-1]; // to remember the size of the kv from
        int packagesize;                // size of receiving package (the kv pairs that slaves send below)
        for (int i = 0; i < world_size-1; i++){ // -1 because we are not doing anything
            MPI_Probe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
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
        kv      package[size];
        int i = 0;
        for (auto resultkv : result){
            strcpy(package[i].key, resultkv.first.c_str());
            package[i++].value = resultkv.second;
        }
        MPI_Send(package, size, kvtype, 0, 0, MPI_COMM_WORLD);
    }
    // FREE TYPE
    MPI_Type_free(&kvtype);
}

void writeToFile(char* filename, const map<string, int> & result){
    /* given a map output, create a file with filename and write wordcount */
    ofstream outfile(filename);
    // since map is already ordered, we just iterate through it and write to file
    for (auto i : result){
        outfile << i.first <<  "\t" << i.second << endl;
    }
    outfile.close();
}

int main(int argc, char ** argv){
    /* Send argv to all */
    MPI_Init(&argc, &argv); // argv will be original link
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Get_processor_name(processor_name, &name_len);

    map<string, int> result;
    queue<char *> workqueue;

    printf("Hello this is processor %s, rank %d.\n", processor_name, rank);
    start(argv[1]);
    /* FIRST DISTRIBUTE WORK */
    if (rank == 0){
        /* Setup job... open argv[1] (original dir), obtain paths of containing files */
        map<string,int> output;
        printf("Processor %s, rank %d: Starting with %s\n", processor_name, rank, argv[1]);
        masterSendPath(argv[1], workqueue);
        // receive til finish
    }
    else{ // slave
        receiveWork(workqueue);
    }
    // NOW EACH NODE HAS A WORKQUEUE... WE WILL MAP THEN REDUCE
    slaveMap(result, workqueue);
    // wait done mapping... do i need to wait?
    MPI_Barrier(MPI_COMM_WORLD);
    reduce(result); // all process send result to master for reducing
    // finish by writing to file
    if (rank == 0)
        writeToFile(argv[2], result);
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
}



