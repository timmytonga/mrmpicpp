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
- With automatic handling of parallization, load-balancing, and (coming soon...) fault-tolerant, the programs that follow this model written using the MapReduce library are then much simpler, easier to modify and understand. 
- This library has been tested with wordcount. 
### Pros:
- Simple to use: The programmer simply supplies the map and reduce function following the library's specification in order to carry out computation on multiple nodes. 
- Lightweight and portable library. (depend only on MPI and Pthread)

### Cons:
- No fault tolerant.

### To be implemented (! denotes importance):
- Script for better file split -- work balance (!). 
- Fault tolerant. (!!)
- Integration with HDFS (!!!). 
- Real time task status. 
- Default functions for mapper/reducer like identity. 
- Better API --> removal of MPI setup calls.... (retain for better customization). 
- Makefile for easy compilation 
- Master's datastructure to keep track of counts and other data --> Users have the ability to add their own "counter" (similar to the one in the Google MR paper). 
- More I/O options (split file output). 

## Direction (under construction)
### 0. Setting up main, MPI, and MapReduce:
#### a. Main function and MPI:
- Most main functions used with this C++MapReduce library are required to contain the following (to initialize MPI enviroment):
```
int main(int argc, char ** argv){
	int world_size, my_rank; // contains number of processors and program's rank
	MPI_Init(&argc, &argv); 	// don't access argc, argv before this line.
	// below we initialize rank and size 
	MPI_Comm_rank(MPI_COMM_WORLD, &myrank); // be sure to have consistent comm
	MPI_Comm_size(MPI_COMM_WORLD, &world_size); 
	...
}
```
- Afterwards, we need to setup mapreduce with our desired key, value:
```
MapReduce<Key,Value> *mr = new MapReduce<Key,Value>(MPI_Comm, const char * <input_directory>, const char * <output_directory>);
MPI_Barrier(MPI_Comm); //
```

	

### 1. Writing the mapper and reducer function 
##### a. Mapper:
- The purpose of writing the mapper is to process a file into corresponding Key, Value pairs in which the user will emit (for gathering and reducing later on). 
- The format of writing a correct reduce function is the following declaration (with correct types and parameters): 
`void reducer(MapReduce<Key,Value> *mr, const char * path)` 
- Then the user will process the file given by the path and call `mr->emit(Key, Value);` at the end of the function.
- Be sure to close the file and handle any related issues. 
- Please refer to the Wordcount example and the comments for further details.

##### b. Reducer:
- Similar to the mapper function, the user will write a reducer function with the following format:
`void reducer(MapReduce<Key, Value> *mr)`
- The goal of writing the reducer function is to iterate through the Key Value pairs using the default iterator. The user can easily do so by:
```
for (auto kv: mr){
    // kv.first ==> Key and kv.second ==> Vector<Value>
}
```
- Then the user can call `emit_final(Key, Value)` for final output by reducer function. 

### 2. Writing the sorting function 
- The MapReduce library sorts the Key by alphabetical order by default. However, the user can specify how the Key,Value pairs are sorted by writing a custom sorting function in the following format:
` bool sort(Key key1, Key key2)` 
- This function will return the result of the comparison between key1 and key2: false or 0 if key1 is "greater than" key2, and true or 1 otherwise (where greater than means to be appeared later in the result).  

### 3. Compiling 
- Compile using cmake -- type: `cmake <your program> .`
- Or you can compile manually using `mpiCC -std=c++11 <program's name> mapreduce.cpp keyvalue.cpp -o <binary>` 

### 4. Running on single machine 
- `mpirun -np <number_of_processors> <binary> <argv[1]> <argv[2]> ... ` 
- Coming soon: `runmapreduce <your_program> <argv[1]> <argv[2]> ...` --> will process a setting file and performs automatic load balancing. 
 

### 5. Running on cluster -- writing host file and configuring mpirun 
#### a. Write a hostfile:
#### b. Run with hostfile option: 
- `mpirun -np <number_of_processors> --hostfile <name_of_hostfile> <binary> <argv[1]> <argv[2]> ... ` 

### 6. Some common bugs:
#### 1. undefined reference when compiling 
- C++ template sometimes requires expicit instantiation. So if you are encountering `undefined reference` errors when compiling/linking, it's most likely that the `MapReduce<Key, Value>` that you are trying to initialize requires expicit instantiation. 
- There are two ways to circumvent this:
	1. Explicitly instantiate your desired template in the MapReduce.cpp file so the linker knows beforehand. If go all the way to the bottom of the `mapreduce.cpp` source file, there should be a bunch of `MapReduce<std::string, int>` and `MapReduce<const char *, int>` etc. Just add your desired template and recompile everything. 
	2. Use one of the default templates such as `MapReduce<std::string, std::string>` and handle type conversions in your map and reduce functions. This should not hiner performances too much as most compilers will be able to optimize these. 
	

