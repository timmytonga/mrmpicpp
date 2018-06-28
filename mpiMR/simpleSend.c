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
    
    /* First get path length and name length */
    path_max = (size_t)pathconf(argv[1], _PC_PATH_MAX);
    if (path_max == -1){
        if (errno == 0) path_max = 1024;
        else errno_abort("Unable to get PATH_MAX");
    }
    name_max = (size_t)pathconf(argv[1], _PC_NAME_MAX);
    if (name_max == -1){
        if (errno == 0) name_max = 256;
        else errno_abort("Unable to get NAME_MAX");
    }
    // for null char 
    path_max++;
    name_max++;


    printf("Hello this is processor %s, rank %d.\n", processor_name, rank);
    if (rank == 0){ /* Setup job... open argv[1] (original dir), obtain paths of containing files */
        printf("Processor %s, rank %d: Starting with %s\n", processor_name, rank, argv[1]);
    }
    else {
    }
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
}


