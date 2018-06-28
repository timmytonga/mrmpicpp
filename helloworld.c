#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

int main(int argc, char** argv){
    MPI_Init(&argc, &argv);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int name_len;
    MPI_Get_processor_name(processor_name, &name_len);
    
    printf("Hello world from processor %s, rank %d, out of %d processors...\n",
        processor_name, rank, world_size);
    //printf("Input argument is %s\n", argv[1]);
    
    /* communication */
    if (rank == 4){
        char * test  = "hello";
        printf("I am rank 0\n");
        MPI_Send(&test, 7, MPI_CHAR, 5, 0, MPI_COMM_WORLD);
    }
    if (rank == 5){
        char * buf[10];
        MPI_Status status;
        int ierr;
        ierr = MPI_Recv(&buf, 7, MPI_CHAR, 4, 0, MPI_COMM_WORLD, &status);
        if (ierr == MPI_SUCCESS)
            printf("Rank %d (processor %s) received value %s.\n", rank, processor_name, buf);
        else   
            printf("Rank %d (processor %s) did not sucessfully receive a value!\n", rank, processor_name);
    }
    else
        printf("I'm rank %d\n", rank);
    MPI_Finalize();
}
