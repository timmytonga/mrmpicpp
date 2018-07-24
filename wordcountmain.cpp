//
// Created by timmytonga on 7/24/18.
//

#include "mapreduce.h"
#include <fstream>
#include <sys/stat.h>

using namespace MAPREDUCE_NAMESPACE;
using namespace std;
int main(int argc, char ** argv){
    MPI_Init(&argc, &argv);

    int world_size, myrank;
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    if (argc <= 1){
        if (myrank == 0) printf("Usage: ./%s <input directory path> <output path>\n", argv[0]);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }


    MapReduce *mr = new MapReduce<string, int>(MPI_COMM_WORLD, argv[1], argv[2]);


}


void wordcount(MapReduce<string, int> *mr, const char * path){
    /* The purpose of this function is to process the path and emit kv pairs*/
    struct stat filestat;
    int status;
    status = stat(path, &filestat);
    if (S_ISLNK(filestat.st_mode))
        printf("%s is a link. Skipping...\n", path);
    else if (S_ISDIR(filestat.st_mode)){
        // to do  --> master should only send regular files to slaves...
        fprintf(stderr, "%s is a dir! Skipping... \n", path);
    }
    else if (S_ISREG(filestat.st_mode)){
        // if this is a regular file, we want to perform word count on this file,
        //      get the word count in sorted order
        //      then send the words with its data to master for big reduction and output
        //   Make faster: if file size big, split and multi thread the wordcount
        ifstream file;
        file.open(path); // open the file
        if (!file.is_open()){
            fprintf(stderr, "ERROR (Processor %s rank %d): Cannot open file %s\n ", processor_name, nrank, path);
            return;
        }
        string word;
        while(file >> word)  // read words in file
            mr->emit(word, 1); // EMIT KV: (word, 1) -- for collating and reducing later
        file.close();
    }
    else {
        fprintf(stderr, "Error: CANNOT OPEN FILE %s!\n", path);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}