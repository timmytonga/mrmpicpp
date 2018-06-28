// LAST EDIT: JUN 27 2018 BY TIM NG
#define DEBUG // DEBUGGING 

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#include <string.h>
#include "errors.h"

// maximum size for path and name
size_t path_max;
size_t name_max; 
char processor_name[MPI_MAX_PROCESSOR_NAME];
int world_size, rank, name_len;

int main(int argc, char ** argv){
    /* Send argv to all */    
    MPI_Init(&argc, &argv); // argv will be original link 
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Get_processor_name(processor_name, &name_len);
    
    printf("Hello this is processor %s, rank %d.\n", processor_name, rank);
    start(argv[1]);
    if (rank == 0){ /* Setup job... open argv[1] (original dir), obtain paths of containing files */
        printf("Processor %s, rank %d: Starting with %s\n", processor_name, rank, argv[1]);
        masterSendPath(argv[1]);
    }
    else {
        engine();
    }
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
}

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


void masterSendPath(char * path){ // explore path and send workers work 
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
        int slave = 0;  // for sending 1 by 1 
        int dest;
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
            dest = slave + 1;
            DPRINTF(("Sending path %s/%s to %d\n", path, result->d_name, dest));
            MPI_Send(newpath, path_max, MPI_CHAR, dest, 0, MPI_COMM_WORLD);
            slave = (slave+1)%(world_size-1); // update 
        }
        DPRINTF(("MASTER FINISHING...CLOSING DIR....\n"));
        closedir(directory);
    }
    
    //MPI_Bcast( d, 5, MPI_CHAR, 0, MPI_COMM_WORLD);
    for (int i = 1; i < world_size; i++) // signal done 
        MPI_Send("",1, MPI_CHAR, i, 1, MPI_COMM_WORLD);
}

void engine(){
    int ierr;
    //size_t path_len;
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
        }
    }

    DPRINTF(("Processor %s, rank %d: Finish engine\n", processor_name, rank));
}
