# MapReduce C++

- Please follow direction below to write a MapReduce program. For example, checkout wordcountmain.cpp and the following comments. 

### The example:

- Compile (from main dir): 
	`cmake .`
- Compile manual: ` mpiCC -std=c++11 mapreduce.cpp keyvalue.cpp wordcountmain.cpp -o wordcount`
- To run: `mpirun -np <number of processor> ./wordcount <input_dir_path> <output_dir_path>` 
- To turn on/off verbose/debug: Uncomment or comment `#define DEBUG` in mapreduce.cpp file 

## Summary

- Many distributed computational tasks can be done through the map and reduce model (wordcount, distributed sort, grep, etc.). Instead of writing brand new parallel programs for tasks that follow this model again and again, this library removes the burden of communicating between nodes, optimizing parallelization and managing filesystems for the programmer. 
- This library has been tested with wordcount. 
### Pros:
- Simple to use: The programmer simply supplies the map and reduce function following the library's specification in order to carry out computation on multiple nodes. 
- Lightweight and portable library. (depend only on MPI and Pthread)

### Cons:
- No fault tolerant.

### To be implemented:
- Script for better file split -- work balance. 
- Fault tolerant.

## Direction (under construction)
### 0. Setting up main, MPI, and MapReduce:
#### a. Main function and MPI:
- Most main functions used with this C++MapReduce library are required to contain the following (to initialize MPI enviroment):
``` int main(int argc, char ** argv){
	int world_size, my_rank; // contains number of processors and program's rank
	MPI_Init(&argc, &argv); 	// don't access argc, argv before this line.
	// below we initialize rank and size 
	MPI_Comm_rank(MPI_COMM_WORLD, &myrank); // be sure to have consistent comm
	MPI_Comm_size(MPI_COMM_WORLD, &world_size); 
	...
}```
- Afterwards, we need to setup mapreduce with our desired key, value:
```MapReduce<Key,Value> *mr = new MapReduce<Key,Value>(MPI_Comm, const char * <input_directory>, const char * <output_directory>)
MPI_Barrier(MPI_Comm);``` 

	

### 1. Writing the mapper and reducer function 
##### a. Mapper:
- The purpose of writing the mapper is to process a file into corresponding Key, Value pairs in which the user will emit (for gathering and reducing later on). 
- The format of writing a correct reduce function is: 
`void reducer(MapReduce<Key,Value> *mr, const char * path)` 
- The MapReduce library will distribute to each reducer an appropriate path (not necessarily the user's original path -- but the user can assume the path with contain the same content). The user is then expected to process the file and emit appropriated key value pair. 
- An example of a reducer function can be seen in the function `wordcount` in example `wordcountmain.cpp`. 
- The user is expected to handle I/O and other errors between processing the file and emitting KV pairs. 
#####

#### Purpose/target
- The purpose of the mapping function is to 
- Example 
- Options 

### 2. Writing the sorting function 

### 3. Compiling 
- `mpiCC -std=c++11 <program's name> mapreduce.cpp keyvalue.cpp -o <binary>` 

### 4. Running on single machine 
- `mpirun -np <number_of_processors> <binary> <argv[1]> <argv[2]> ... ` 
 

### 5. Running on cluster -- writing host file and configuring mpirun 
#### a. Write a hostfile:
#### b. Run with hostfile option: 
- `mpirun -np <number_of_processors> --hostfile <name_of_hostfile> <binary> <argv[1]> <argv[2]> ... ` 

### 6. Some common bugs:
#### 1. undefined reference when compiling 


