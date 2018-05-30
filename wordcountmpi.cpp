//
// Created by timmytonga on 5/21/18.
//

#include "mpi/mpi.h"
#include <iostream>


int main(int argc, char ** argv){
    int world_size, rank, name_len;
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Get_processor_name(processor_name, &name_len);

    if (rank == 0){ // master
        // get work, start queue and distribute work...
        argv[1]; // this is starting queue
    }
    else{ // worker

    }
    printf("Hello world from processor %s, rank %d, out of %d processors\n", processor_name, rank, world_size);


    MPI_Finalize();
    return 0;
}
