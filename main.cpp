//
// Created by timmytonga on 5/17/18.
//

#define DEBUG    // for debugging

extern "C"
{
#include "workq.h"
}

#include <iostream>     // io
#include <fstream>      // for file processing
#include <map>          //map data structure for wordcount
/* DIRECTORY AND ERROR CHECKING INCLUDES */
#include <sys/stat.h>
#include <dirent.h>
#include "errors.h"
#include <unistd.h>
/* THREADED */
#define MAXTHREADS 5

workq_t workqueue;

size_t path_max;
size_t name_max;
// synchronization
pthread_cond_t done;
pthread_mutex_t countLock;
int work_count; // count work
using namespace std;

/* GLOBAL OUTPUT DICT */
map<string, int> output;

map<string,int> wordcount(const char * filepath){       // this is like mapper
    /* Returns a map with the appropriate word count so that can add to final result */
    map<string, int> result;
    ifstream file;
    file.open(filepath); // open the file
    if (!file.is_open()){
        cerr << "ERROR: Cannot open file " << filepath << endl;
        return result;
    }
    string word;
    while(file >> word) { // read words in file
        result[word]++; // increment count in result
    }
    file.close();
    return result;
};

void writeToFile(char* filename){
    /* given a map output, create a file with filename and write wordcount */
    ofstream outfile(filename);
    // since map is already ordered, we just iterate through it and write to file
    for (auto i : output){
        outfile << i.first <<  " " << i.second << endl;
    }
    outfile.close();
}

void engine(void * arg){
    char * path = (char*) arg;
    DPRINTF(("Current path %s\n", path));
    // first check if dir... then queue the files and recursively perform
    struct stat filestat;   // file stat
    int status;             // C error checking

    status = stat(path, &filestat);
    //if (status != 0) err_abort(status, "lstat engine");

    if (S_ISLNK(filestat.st_mode))
        cout << path << "is a link. Skipping...\n";

    if (S_ISREG(filestat.st_mode)) // if it's a file then we just run it and output the result
    {
        size_t len = strlen(path);
        const char * ext = &path[len-4];
        DPRINTF((">>>> Extension is %s\n", ext));

        map<string, int> temp = wordcount(path); //map
        // modifying the main output so we lock mutex
        pthread_mutex_lock(&countLock);
        for (auto kv: temp)
            output[kv.first] += kv.second;
        pthread_mutex_unlock(&countLock);

    }

    else if (S_ISDIR(filestat.st_mode)){
        DIR *directory;
        struct dirent* result;
        directory = opendir(path);
        if (directory == nullptr){ // error opening dir
            cerr << "Unable to open directory " << path << ": " << errno << strerror(errno) << endl;
            delete path;
            return;
        }
        while(true){
            errno = 0;
            result = readdir(directory);
            if (result == nullptr){
                if (errno != 0) errno_abort("Readdir during check directory");
                break; // end of dir
            }
            // skip . and ..
            if (strcmp(result->d_name, ".") == 0) continue;
            if (strcmp(result->d_name, "..") == 0) continue;

            // ADD WORK TO WORKQ SO MANAGER STARTS MORE THREADS
            char * newpath = new char[path_max];
            DPRINTF(("> Found new work! Adding path: %s/%s\n", path, result->d_name));
            strcpy(newpath, path);
            strcat(newpath, "/");
            strcat(newpath, result->d_name);
            workq_add(&workqueue, (void*)newpath);

            pthread_mutex_lock(&countLock);
            work_count++;
            pthread_mutex_unlock(&countLock);
        }
        closedir(directory);
    }

    else cerr << "ERROR: Cannot find dir " << path << endl;
    delete path;
    status = pthread_mutex_lock(&countLock);
    if (status != 0) err_abort(status, "Lock count mutex");
    work_count--;
    DPRINTF(("Decremented to work %d\n", work_count));
    if (work_count <= 0) // if no more work to be done
    {
        DPRINTF(("DONE --> signaling manager to destroy workqueue\n"));
        status = pthread_cond_broadcast(&done);
        if (status != 0) err_abort(status, "Broadcast done");
    }
    status = pthread_mutex_unlock(&countLock);
    if (status != 0 ) err_abort(status, "Unlock count mutex");
}

int start(workq_t * wq, int numthreads, char * filepath){
    // our work request is just filepath
    int status;
    char * request;
    status = pthread_cond_init(&done, nullptr);
    if (status != 0) return status;
    status = pthread_mutex_init(&countLock, nullptr);
    if (status != 0) return status;
    status = workq_init(&workqueue, numthreads, &engine);
    if (status !=0) err_abort(status, "Initialize workqueue in main \n");
    // configure pathmax and namemax
    errno = 0;
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
    DPRINTF(("PATH_MAX for %s is %ld, NAME_MAX is %ld\n", filepath, path_max, name_max));
    path_max++; name_max++; // for null bytes
    request = new char[path_max]; // deallocate in engine
    strcpy(request, filepath);
    /* ADD WORK THEN WAIT FOR COMPLETION */
    status = pthread_mutex_lock(&countLock);
    if (status != 0) return status;
    work_count++;
    // below starts the engine with the first request (see detailed implementation in workq.c)
    // it will add work to the queue if required and continue working until signaled done
    status = workq_add(wq, (void*)request);
    if (status != 0) err_abort(status, "Error adding request at start");
    /* WAIT */
    status = pthread_cond_wait(&done, &countLock);
    if (status != 0) err_abort(status, "Wait");
    status = pthread_mutex_unlock(&countLock);
    if (status != 0) return status;
    /* DONE */
    status = workq_destroy(&workqueue);
    if (status != 0) err_abort(status, "destroy work queue");
    return 0;
}

int main(int argc, char** argv){
    /* Takes a dir path in argv[1] and recursively check that path for files and
     * output a file with name argv[2] with the words followed by a number as count */
    if (argc != 3){
        cout << "argc = " << argc << endl << "Usage: ./wordcount DIR_PATH OUTFILENAME\n";
        exit(1);
    }
    // map for outputting

    // threading
    int numthreads = MAXTHREADS;
    char * startpath = argv[1];     // start path

    clock_t t;
    t = clock();
    start(&workqueue, numthreads, startpath); // start is a path to a dir to count words
    t = clock() - t;
    double time = ((double)t)/CLOCKS_PER_SEC;

    cout << "\n It took " << time << " seconds to compute with " << numthreads << " threads.\n";

    writeToFile(argv[2]);   // argv[2] is destination's file path.
    return 0;
}