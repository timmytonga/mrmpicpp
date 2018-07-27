//
// Created by timmytonga on 7/24/18.
//

#include "mapreduce.h"
#include <fstream>
#include <sys/stat.h>

using namespace MAPREDUCE_NAMESPACE;

void wordcount(MapReduce<std::string, int> *mr, const char * path);
void output(MapReduce<std::string,int> *mr);

int main(int argc, char ** argv){
    /* The initialization phase... All main programs will start roughly the same
     * We are initializing the communnication environment (MPI) */
    MPI_Init(&argc, &argv);
    int world_size, myrank;
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    if (argc != 3){
        if (myrank == 0) printf("Usage: ./%s <input directory path> <output path>\n", argv[0]);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* Now this is the meat of the program. We start by creating a MapReduce object (avoid creating it on the stack to prevent stackoverflow)
     * Then we will run the mapper and reducer with our custom written function */
    MapReduce<std::string, int> *mr = new MapReduce<std::string, int>(MPI_COMM_WORLD, argv[1], argv[2]);
    MPI_Barrier(MPI_COMM_WORLD);    // wait for all nodes to finish initializing before beginning map phase
    /* Map phase. Refer to the wordcount function for how to write a mapper function */
    mr->mapper(wordcount);      // pass in our mapping function that will emit appropriate key value pairs.
    mr->sort_and_shuffle();     // here we can pass in a function that will arrange how the keys can be sorted
    // here we reduce by running output on each node, send the local result to master (rank 0)
    // and then output to file
    mr->reducer(output);

    /* Necessary final steps: join and clean up */
    MPI_Barrier(MPI_COMM_WORLD);
    delete mr;
    MPI_Finalize();
}


void wordcount(MapReduce<std::string, int> *mr, const char * path){
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
        std::ifstream file;
        file.open(path); // open the file
        if (!file.is_open()){
            fprintf(stderr, "ERROR (Processor %s rank %d): Cannot open file %s\n ", mr->get_processor_name(), mr->get_nrank(), path);
            return;
        }
        std::string word;
        while(file >> word)  // read words in file
            mr->emit(word, 1); // EMIT KV: (word, 1) -- for collating and reducing later
        file.close();
    }
    else {
        fprintf(stderr, "Error: CANNOT OPEN FILE %s!\n", path);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

int sum_vector(std::vector<int> v){ // reducer function
    int result = 0 ;
    for (auto i : v)
        result += i;
    return result;
}

void output(MapReduce<std::string, int> *mr){
    // writing this function requires the user to access MapReduce's iterator
    // the kv pairs have been collated in the form of a pair<Key, Vector<Value>>
    // so the user will decide on what to do with the Vector of Values.
    for (const auto &kv : *mr){ // kv will be in the form of (Key, Vector<Value>)
        // the user's job is to decide how to reduce it to (Key, Value) pairs and call emit_final
        // emit final will output sorted kv pairs to file...
        mr->emit_final(kv.first, sum_vector(kv.second));
    }
}

