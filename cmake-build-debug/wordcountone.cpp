//
// Created by timmytonga on 5/14/18.
//
#define DEBUG    // for debugging

#include <iostream>     // io
#include <fstream>      // for file processing
#include "string"       // string
#include "map"          //map data structure for wordcount
#include "queue"
/* DIRECTORY AND ERROR CHECKING INCLUDES */
#include <sys/stat.h>
#include <dirent.h>
#include "errors.h"

using namespace std;

map<string,int> wordcount(const char * filepath){
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

void writeToFile(const map<string,int> & output, char* filename){
    /* given a map output, create a file with filename and write wordcount */
    ofstream outfile(filename);
    // since map is already ordered, we just iterate through it and write to file
    for (auto i : output){
        outfile << i.first <<  " " << i.second << endl;
    }
    outfile.close();
}

void processFile(map<string,int> & output, queue<string> & workq){
    // fill given maps with wordcount of path
    string a = workq.front();
    const char * path = a.c_str();
    workq.pop();
    DPRINTF(("Current path %s\n", path));
    // first check if dir... then queue the files and recursively perform
    struct stat filestat;   // file stat
    int status;             // C error checking

    status = stat(path, &filestat);
    //if (status != 0) err_abort(status, "lstat engine");

    if (S_ISLNK(filestat.st_mode))
        cerr << path << "is a link. Skipping...\n";

    if (S_ISREG(filestat.st_mode)) // if it's a file then we just run it and output the result
    {
        map<string,int> temp = wordcount(path);
        for (auto kv: temp)
            output[kv.first] += kv.second;
    }
    else if (S_ISDIR(filestat.st_mode)){
        DIR *directory;
        struct dirent* result;
        directory = opendir(path);
        if (directory == nullptr){ // error opening dir
            cerr << "Unable to open directory " << path << ": " << errno << strerror(errno) << endl;
            exit(1);
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

            // give the workq more work

            string toPush = path;
            toPush += "/";
            toPush += result->d_name;

            workq.push(toPush);
            DPRINTF((">>Added %s\n", result->d_name));
        }
        closedir(directory);
    }

    else cerr << "ERROR: Cannot find dir " << path << endl;

}

int main(int argc, char** argv){
    /* Takes a dir path in argv[1] and recursively check that path for files and
     * output a file with name argv[2] with the words followed by a number as count */
    if (argc != 3){
        cout << "argc = " << argc << endl << "Usage: ./wordcount DIR_PATH OUTFILENAME\n";
        exit(1);
    }
    // map for outputting
    map<string,int> output;
    queue<string> workq;

    string start = argv[1];
    workq.push(start);

    while (!workq.empty()){ // while not empty
        processFile(output, workq);
    }

    writeToFile(output, argv[2]);
    return 0;
}