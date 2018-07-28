# mpimrcpp

- Please follow direction below to write a MapReduce program. For example, checkout wordcountmain.cpp and the following comments. 

### The example:

- Compile (from main dir): 
	`cmake .`
- Compile manual: ` mpiCC -std=c++11 mapreduce.cpp keyvalue.cpp wordcountmain.cpp -o wordcount`
- To run: `mpirun -np <number of processor> ./wordcount <input_dir_path> <output_dir_path>` 
- To turn on/off verbose/debug: Uncomment or comment `#define DEBUG` in mapreduce.cpp file 

# Summary

- Many distributed computational tasks can be done through the map and reduce model (wordcount, distributed sort, grep, etc.). Instead of writing brand new parallel programs for tasks that follow this model again and again, this library removes the burden of communicating between nodes, optimizing parallelization and managing filesystems for the programmer. 
- This library has been tested with wordcount. 
### Pros:
- Simple to use: The programmer simply supplies the map and reduce function following the library's specification in order to carry out computation on multiple nodes. 
- Lightweight and portable library. (depend only on MPI and Pthread)

### Cons:
- No fault tolerant.

### To be implemented:
- Script for better file split -- work balance. 

# DIRECTION
- Under construction! 
