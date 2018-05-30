//
// Created by timmytonga on 5/7/18.
//

#include <iostream>
#include "mpi.h"

#define NPROCS 8

/* Run with a directory as an input
 * Take a file */
int main(int argc, char ** argv){
    int rank, new_rank, sendbuf, recvbuf, numtasks;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);

    if (numtasks != NPROCS && rank == 0){
        printf("Must specify MP_PROCS = %d.\n", NPROCS);
        MPI_Finalize();
        exit(0);
    }

    sendbuf = rank;



    MPI_Finalize();
    return 0;
}
