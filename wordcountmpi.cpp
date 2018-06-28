//
// Created by timmytonga on 5/21/18.
//

#include "mpi/mpi.h"
#include <iostream>
#include <string>
#include <queue>
#include <unistd.h>
#include "errors.h"
using namespace std;

void map();
void reduce();
size_t path_max;

int main(int argc, char ** argv){
    int world_size, rank, name_len;
    char processor_name[MPI_MAX_PROCESSOR_NAME];

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Get_processor_name(processor_name, &name_len);

    if (rank == 0){ // master
        // get work, start queue and distribute work...
        //queue<string> work;
        char* start = argv[1]; // this is starting queue
        // get maximum path length to send string
        path_max = (size_t)pathconf(start, _PC_PATH_MAX);
        if (path_max== -1){
            if (errno == 0) path_max = 1024;
            else errno_abort("Unable to get PATH_MAX");
        }
    }
    else{ // worker
        char * buf;
        // MPI_Recv(buf, 1, MPI_CHAR);


    }
    printf("Hello world from processor %s, rank %d, out of %d processors\n", processor_name, rank, world_size);


    MPI_Finalize();
    return 0;
}
