//
// Created by timmytonga on 5/7/18.
//

#define DEBUG

#include "mapreduce.h"
#include "errors.h"
#include "unistd.h"
#include <sys/stat.h>
#include <dirent.h>
#include <fstream>


namespace MAPREDUCE_NAMESPACE {

    template<class Key, class Value>
    MapReduce<Key, Value>::MapReduce(MPI_Comm communicator, char *inpath, char *outpath)
            : comm(communicator), inputPath(inpath), outputPath(outpath) {
        int name_len;
        keyValue = new KeyValue<Key, Value>;
        MPI_Comm_rank(comm, &nrank);
        MPI_Comm_size(comm, &world_size);
        setup_machine_specifics();
        processor_name = new char[name_max];
        MPI_Get_processor_name(processor_name, &name_len);
        DPRINTF(("IN CONSTRUCTOR: Hello this is processor %s, nrank %d.\n", processor_name, nrank));
        /* Send files of equal sizes to each node to prepare for mapping */
        distributeWork();
    }

    template<class Key, class Value>
    MapReduce<Key, Value>::~MapReduce() {
        // deallocate stuff
        delete processor_name;
        delete keyValue;
    }

    template<class Key, class Value>
    void MapReduce<Key, Value>::distributeWork() {
        if (nrank == 0) {
            /* Setup job... open argv[1] (original dir), obtain paths of containing files */
            masterSendPath();
            // receive til finish
        } else { // slave
            receiveWork();
        }
    };

    template<class Key, class Value>
    void *MapReduce<Key, Value>::engine(void *f) {
        // distribute task and then run mapper on function f
        std::string temp;
        while (1) { // while not empty
            pthread_mutex_lock(&countLock);
            if (workqueue.empty()) {
                pthread_mutex_unlock(&countLock);
                break;
            }
            temp = workqueue.front();
            workqueue.pop();
            pthread_mutex_unlock(&countLock);
            // cast the function passed to engine back to the function and run
            ((void (*)(MapReduce<Key, Value> *, const char *)) f)(this, temp.c_str());
        }
    };


    template<class Key, class Value>
    void MapReduce<Key, Value>::mapper(void (*f)(MapReduce<Key, Value> *, const char *)) {
//        /* We use threads to further parallize */
//        DPRINTF((" @@IN MAP: Processor %s, nrank %d: workqueue %s\n ", processor_name, nrank, workqueue.front().c_str()));
//        int status, numthreads;
//        int worksize = workqueue.size();
//        status = pthread_mutex_init(&countLock, nullptr);
//        if (status != 0) err_abort(status, "Initialize countLock in start \n");
//        numthreads = worksize < MAXTHREADS ? worksize : MAXTHREADS;
//        pthread_t * threads = new pthread_t[numthreads];
//        for (int i = 0; i < numthreads; i++) {
//            status = pthread_create(&threads[i], NULL, MapReduce<Key,Value>::engine, f);
//            if (status != 0) err_abort(status, "Create worker");
//        }
//        for (int i = 0; i < numthreads; i++) {
//            status = pthread_join(threads[i], NULL);
//            if (status != 0) err_abort(status, "Joining workers");
//        }
//        delete threads;
        std::string temp;
        while (1) { // while not empty
            if (workqueue.empty()) {
                break;
            }
            temp = workqueue.front();
            workqueue.pop();
            // cast the function passed to engine back to the function and run
            f(this, temp.c_str());
        }
    }


    template<class Key, class Value>
    void MapReduce<Key, Value>::reduceCommunication() { /* reducer uses this to send result from slaves to master */
        MPI_Datatype kvtype = register_kv_type();
        if (nrank == 0) {
            MPI_Status status, status2;
            /* FIRST COLLECT PACKAGES FROM SLAVES ...  */
            kv *collect[world_size - 1];     // array to store results from slaves
            int collectsize[world_size - 1]; // to remember the size of the kv from
            int packagesize;                // size of receiving package (the kv pairs that slaves send below)
            for (int i = 0; i < world_size - 1; i++) { // -1 because we are not doing anything
                MPI_Probe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status); // PROBE to get package size
                MPI_Get_count(&status, kvtype, &packagesize);
                collect[i] = new kv[packagesize]; // remember to free after done counting and adding to result !!!!
                collectsize[i] = packagesize;
                MPI_Recv(collect[i], packagesize, kvtype, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status2);
            }
            // now collect has a bunch of kv arrays. we add each on result (can parallelize with threads)
            for (int i = 0; i < world_size - 1; i++) {
                for (int j = 0; j < collectsize[i]; j++) {
                    kv temp = collect[i][j];
                    // Here master's keyValue get slaves' new values !!! IMPORTANT!!!
                    keyValue->add_kv(temp.key,
                                     temp.value); // this add to the (Key, Vector<Value>) pair for final reduction
                }
                // now we are done with that collect
                delete[] collect[i];
            }
        } else { // send pairs of data --> master gather these pairs and process
            /* PACKAGE THE MAP AND THEN SEND IT TO MASTER */
            long size = result.size();
            kv *package = new kv[size];
            int i = 0;
            for (auto resultkv : result) {
                strcpy(package[i].key, resultkv.first.c_str());
                package[i++].value = resultkv.second;
            }
            MPI_Send(package, size, kvtype, 0, 0, MPI_COMM_WORLD);
            delete[] package;
        }
        // FREE TYPE
        MPI_Type_free(&kvtype);
    }

    template<class Key, class Value>
    void MapReduce<Key, Value>::reducer(void (*f)(MapReduce<Key, Value> *)) {
        if (nrank != 0) {// first we reduce locally on slave nodes
            f(this); // hopefully call emit_final and we have a finished keyValue object.
            result = keyValue->get_result(); // so now each node will have the local result to send to master
        }
        // the function below will send slaves' data to master
        reduceCommunication(); // master's kv will receive new kv values
        if (nrank == 0) {
            f(this); // now we emit final in master's node and have a finished master's keyValue object which is our final result
            result = keyValue->get_result(); // this is master's result
            write_to_file();                   // write result map to file specified by output path
        }

    }

    template<class Key, class Value>
    void MapReduce<Key, Value>::emit(Key k, Value v) {
        // send the k, v pair to the KeyValue class and store them in a special way for collating and reducing later
        keyValue->add_kv(k, v);
    }

    template<class Key, class Value>
    void MapReduce<Key, Value>::emit_final(Key k, Value v) {
        keyValue->add_kv_final(k, v);
    };

    template<class Key, class Value>
    void MapReduce<Key, Value>::sort_and_shuffle(bool (*compare)(Key, Key)) {
        // to do
        if (compare == NULL) DPRINTF(("Debug: in sort_and shuffle NULL\n"));

    }

/* PRIVATE FUNCTIONS */
    template<class Key, class Value>
    void MapReduce<Key, Value>::setup_machine_specifics() {
        // set up stuff
        path_max = (size_t) pathconf(inputPath, _PC_PATH_MAX);
        if (path_max == -1) {
            if (errno == 0) path_max = 1024;
            else
                errno_abort("Unable to get PATH_MAX");
        }
        name_max = (size_t) pathconf(inputPath, _PC_NAME_MAX);
        if (name_max == -1) {
            if (errno == 0) name_max = 256;
            else {
                fprintf(stderr, "Input path is %s\n. Name max is %d\n",inputPath, name_max);
                errno_abort("Unable to get NAME_MAX");
            }
        }
        // for null char
        path_max++;
        name_max++;
    }


    template<class Key, class Value>
    void MapReduce<Key, Value>::distributeTask(char *dirPath) {
        long totalSize = 0;
        int numFiles, filesPerTask, remainder, sum = 0;

        std::vector<fileInfo> workVector; // datastructure we use to store fileInfo for further processing
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
                    std::string newPath(dirPath); // this is our new path to our file
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
        filesPerTask = numFiles / world_size;
        remainder = numFiles % world_size;

        int *sendcounts = new int[world_size];
        int *displacement = new int[world_size];
        // calculate size and displacements for scattering
        for (int i = 0; i < world_size; i++) {
            sendcounts[i] = filesPerTask;
            if (remainder > 0) {
                sendcounts[i]++;
                remainder--;
            }
            displacement[i] = sum;
            sum += sendcounts[i];
        }

        //MPI_Scatterv(&workVector[0]); // this is the array of size numFiles
    }

    template<class Key, class Value>
    void MapReduce<Key, Value>::masterSendPath() {
        struct stat filestat;
        int status;

        status = stat(inputPath, &filestat);
        if (status != 0) fprintf(stderr, "ERROR at stat(inputFile): %d", status);
        // only process directory and obtain files from dir
        if (S_ISDIR(filestat.st_mode)) {
            DIR *directory;
            struct dirent *resultd;
            directory = opendir(inputPath);
            if (directory == NULL) {
                fprintf(stderr, "Unable to open directory\n");
                return;
            }
            int dest = 0;  // for sending 1 by 1
            while (1) {
                resultd = readdir(directory);
                if (resultd == NULL) {
                    break; // end of dir
                }
                // skip . and ..
                if (strcmp(resultd->d_name, ".") == 0) continue;
                if (strcmp(resultd->d_name, "..") == 0) continue;

                char newpath[path_max];
                strcpy(newpath, inputPath);
                strcat(newpath, "/");
                strcat(newpath, resultd->d_name);
                // now we send the newpath
                if (dest == 0) {
                    DPRINTF(("Sending path %s/%s to %d\n", inputPath, resultd->d_name, dest));
                    workqueue.push(newpath);
                } else {
                    DPRINTF(("Sending path %s/%s to %d\n", inputPath, resultd->d_name, dest));
                    MPI_Send(newpath, path_max, MPI_CHAR, dest, 0, MPI_COMM_WORLD);
                }

                /* OPTIMIZE BY SENDING A BUNCH OF PATHS TOGETHER -- PARTITION FIRST
                 * AND THEN SEND ONCE TO EACH NODE
                 *  --> WANT TO MINIMIZE COMMUNICATIONS (THE NUMBER OF SEND/RECV CALLS */
                dest = (dest + 1) % (world_size); // update
            }
            DPRINTF(("MASTER FINISHING...CLOSING DIR....\n"));
            closedir(directory);
        } else {
            fprintf(stderr, "ERROR: Input directory only.\n");
            MPI_Finalize();
            exit(1);
        }

        //MPI_Bcast( d, 5, MPI_CHAR, 0, MPI_COMM_WORLD);
        for (int i = 1; i < world_size; i++) // signal done
            MPI_Send("", 1, MPI_CHAR, i, 1, MPI_COMM_WORLD); // tag 1 for done while tag 0 for work
    }

    template<class Key, class Value>
    void MapReduce<Key, Value>::receiveWork() {
        int ierr;
        MPI_Status status;
        char buf[path_max];
        while (1) {
            /*MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_CHAR, &path_len);
            if (path_len == 1) break; // signal (empty str) */
            ierr = MPI_Recv(&buf, path_max, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            if (ierr != 0) fprintf(stderr, "Error: MPI Receive in receive work: %s \n", strerror(ierr));
            //DPRINTF(("Processor %s, nrank %d: waiting to receive...\n", processor_name, nrank));
            if (status.MPI_TAG == 1) {
                break;
            } else {
                workqueue.push(std::string(buf));
                DPRINTF(("Processor %s, rank %d: Just pushed %s to front of queue\n", processor_name, nrank, workqueue.front().c_str()));
            }
        }
    }

    template<class Key, class Value>
    MPI_Datatype MapReduce<Key, Value>::register_kv_type() { // do i need this?
        const int structlen = 2;
        int lengths[structlen] = {MAX_WORD_LEN, 1};
        MPI_Aint offsets[structlen] = {offsetof(kv, key), offsetof(kv, value)};
        MPI_Datatype types[structlen];
        if (std::is_same<Key, std::string>::value && std::is_same<Value, int>::value) {
            types[0] = MPI_CHAR;
            types[1] = MPI_INT;
        } else // todo soon!!!
            fprintf(stderr, "Warning: not correct type.... registerkvtype.\n");
        MPI_Datatype kvtype;
        MPI_Type_struct(structlen, lengths, offsets, types, &kvtype);
        MPI_Type_commit(&kvtype);
        return kvtype;
    }

    template<class Key, class Value>
    void MapReduce<Key, Value>::write_to_file() {
        /* given a map output, create a file with filename and write wordcount */
        std::ofstream outfile(outputPath);
        // since map is already ordered, we just iterate through it and write to file
        for (auto i : result) {
            outfile << i.first << "\t" << i.second << std::endl;
        }
        outfile.close();
    }

/* ITERATOR TO ITERATE THROUGH KV PAIRS */
    template<class Key, class Value>
    typename MapReduce<Key, Value>::Iterator MapReduce<Key, Value>::begin() {
        return Iterator(keyValue, 0);
    };

    template<class Key, class Value>
    typename MapReduce<Key, Value>::Iterator MapReduce<Key, Value>::end() {
        return Iterator(keyValue, -1); // for end
    };

    template<class Key, class Value>
    MapReduce<Key, Value>::Iterator::Iterator(KeyValue<Key, Value> *kv, int start) {
        if (start == 0)
            current = kv->begin();
        else
            current = kv->end();
    };


    template<class Key, class Value>
    std::pair<Key, std::vector<Value>> MapReduce<Key, Value>::Iterator::operator*()  {
        return *current;
    };

    template<class Key, class Value>
    typename MapReduce<Key, Value>::Iterator &MapReduce<Key, Value>::Iterator::operator++() {
        ++current;
        return *this;
    };

    template<class Key, class Value>
    typename MapReduce<Key, Value>::Iterator MapReduce<Key, Value>::Iterator::operator++(int) {
        current++;
        return *this;
    };

    template<class Key, class Value>
    bool MapReduce<Key, Value>::Iterator::operator==(const MapReduce<Key, Value>::Iterator &rhs) const {
        return current == rhs.current;
    };

    template<class Key, class Value>
    bool MapReduce<Key, Value>::Iterator::operator!=(const MapReduce<Key, Value>::Iterator &rhs) const {
        return current != rhs.current;
    };


    template<class Key, class Value>
    MapReduce<Key, Value>::Iterator::~Iterator() {};

    /* We have to define this so the linker won't complain ... */
    template class MapReduce<std::string, int>;
}