# mpimrcpp

- Compile wordcountmpi.cpp (main file)
- Run with runwc 

# Summary

- Many distributed computational tasks can be done through the map and reduce model (wordcount, distributed sort, grep, etc.). Instead of writing brand new parallel programs for tasks that follow this model again and again, this library removes the burden of communicating between nodes, optimizing parallelization and managing filesystems for the programmer. 
- The programmer simply supplies the map and reduce function following the library's specification in order to carry out computation on multiple nodes. 
- This library has been tested with wordcount. 
- No fault tolerance yet (detecting hang or CPU failure in MPI is still a lot of work in progress). 
- Lightweight and portable library.
