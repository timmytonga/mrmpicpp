//
// Created by timmytonga on 5/7/18.
//


#include "mapreduce.h"
#include "mpi.h"
#include "errors.h"
#include "unistd.h"
#include <sys/stat.h>
#include <dirent.h>


using namespace MAPREDUCE_NAMESPACE;

template <class Key, class Value>
MapReduce<Key, Value>::MapReduce(MPI_Comm communicator, char* inpath, char* outpath)
: comm(communicator), inputPath(inpath), outputPath(outpath)
{
    keyValue = new KeyValue<Key, Value>;
    MPI_Comm_rank(comm, &nrank);
    MPI_Comm_size(comm, &world_size);
    MPI_Get_processor_name(processor_name, &name_len);
    setup_machine_specifics();
    DPRINTF(("IN CONSTRUCTOR: Hello this is processor %s, nrank %d.\n", processor_name, nrank));
}

template <class Key, class Value>
MapReduce<Key,Value>::~MapReduce() {
    // deallocate stuff
    delete keyValue;
}

template <class Key, class Value>
void MapReduce<Key, Value>::mapper(void (*f)(char *)) {
    // distribute task and then run mapper on function f
}

template <class Key, class Value>
void MapReduce<Key, Value>::reducer(void (*f)(char *) ) {

}

template <class Key, class Value>
void MapReduce<Key, Value>::emit(Key k, Value v) {
    // send the k, v pair to the KeyValue class and store them in a special way for collating and reducing later

}

template <class Key, class Value>
void MapReduce<Key, Value>::sort_and_shuffle(bool (*compare)(Key, Key) = NULL) {

}

/* PRIVATE FUNCTIONS */
void MapReduce::setup_machine_specifics() {
    // set up stuff
    path_max = (size_t)pathconf(inputPath, _PC_PATH_MAX);
    if (path_max == -1){
        if (errno == 0) path_max = 1024;
        else errno_abort("Unable to get PATH_MAX");
    }
    name_max = (size_t)pathconf(inputPath, _PC_NAME_MAX);
    if (name_max == -1){
        if (errno == 0) name_max = 256;
        else errno_abort("Unable to get NAME_MAX");
    }
    // for null char
    path_max++;
    name_max++;
}

void MapReduce::distributeTask(char *dirPath) {
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

void MapReduce::masterSendPath(char *path, queue<string> &masterWQ) {
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

void MapReduce::receiveWork() {
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

MPI_Datatype MapReduce::register_kv_type() { // do i need this?
    const int       structlen = 2;
    int             lengths[structlen] = { MAX_WORD_LEN , 1};
    MPI_Aint        offsets[structlen] = {offsetof(kv, key), offsetof(kv, value)};
    MPI_Datatype    types[structlen] = {MPI_CHAR, MPI_INT};
    MPI_Datatype kvtype;
    MPI_Type_struct(structlen, lengths, offsets, types, &kvtype);
    MPI_Type_commit(&kvtype);
    return kvtype;
}
