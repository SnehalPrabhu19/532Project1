# CS532_F21_Project1 Design Document

## Design Document
## By: Rishabh Garodia, Snehal Prabhu, Shruti Shelke



**Mapper Class**

*src/mapreduce/Mapper.java* :

This is the mapper class for our map reduce implementation. It first makes a connection to the communication channel via the messenger class using the defined socket and port. Through this channel, it receives information needed by the mapper that is common for all mapper instances such as number of mappers, input file path and the user defined mapper class. It takes in offsets which describe the partition of the input file that this mapper has to read, a specific mapper id for identification, path for the user defined map function and number of mappers and initializes these values in its main function. It also creates a directory to store intermediate files. It reads the input from the input file according to the partition it was assigned, applies the udf map function and writes to temporary files the list of pairs which are the output of the udf map function. It also prints out if the mapper was completed successfully.




**Reducer Class**

*src/mapreduce/Reducer.java* :

This is the reducer class for our map reduce implementation. It first makes a connection to the communication channel via the messenger class using the defined socket and port. Through this channel, it receives information needed by the reducer that is common for all reducer instances such as output file path and the user defined reducer class. It gets the temporary file path where the output from the mapper function is kept, the specific reducer id of that reducer for identification, the path for the user defined reduce function, and the path for where it has to store the output. It first reads the intermediate files as strings and converts them back to lists. It picks the keys it has to read for its partition, does the reading and then sorts and shuffles the data according to the relevant keys. It aggregates the list of pairs at the pair.getKey level and converts the TreeMap back to a list of pairs. This reducer then defines the directory for output and then applies the user defined reduce function on the data it reads and begins writing this output to the output file. It also prints out if the reducer was completed successfully.




**Pair Class**

*src/mapreduce/Pair.java* :

This is the pair class that is used by the mapper and reducer classes in our map reduce implementation. It has a key variable and value variable. In this class, we have methods that can get the key, set the key, get the Value, set the Value and convert the key and the value to a string.




**Context Class**

*src/mapreduce/context.java* :

This is the context class of our map reduce library. This class basically defines a list of string and integer pairs called outputData. We can either read the output data or write to it. It is called by the other classes in our map reduce library.




**Utility Function Class**

*src/mapreduce/utils.java* :

This is the utility function class of our map reduce library. We defined this class to have all the static routines that can be used by all the instances of MapReduce. We have a method written that gives us the file extension for any input data file. It takes the file name as its argument. We have methods for creating and deleting directories and one for writing output data, which are all used by the mapper and reducer classes. The write temp data and read temp data methods are responsible for reading and writing the intermediate files by the reducers and mappers respectively. The readOffset method assists in the reading of partitions for each mapper and reducer. The execute method is for the initialization of a process for each mapper and reducer. The utils class also has methods for returning class names and methods for the user defined functions. The last method in the utility class is responsible for reading input data and dividing it into partitions as need be for all mappers and reducers. It also has a mapper fault tolerance method that restarts a mapper if it has not received exit status 0 i.e., it did not properly complete its function and then waits for all the restarted mappers to be completed. Similarly, it has a reducer fault tolerance method that restarts a reducer if it has not received exit status 0 i.e., it did not properly complete its function and then waits for all the restarted reducers to be completed.




**Messenger for Communication Class**

*src/mapreduce/messenger.java* :

This messenger class provides the main communication between mappers, reducers and our master mapreduce class. The connectMapper method provides the main communication channel for mappers where the socket is set to ‘8888’. Shared information for all mappers such as the input file path, user defined mapper class and number of mappers is sent to them by this method after connection has been established. It also maintains a list of all the intermediate files created by the mappers. Similarly, the connectReducer serves as the main communication channel for reducers where the socket is set to ‘6666’. Shared information for all reducers such as the output file path, user defined reducer class is sent to them by this method after connection has been established. Furthermore, we define functions to get file paths for both intermediate and output files and we also create a method to create data streams including setting up the host and port for the sockets.




**Master MapReduce Class**

*src/mapreduce/mapreduce.java* :

This is our main class in our map reduce library, essentially working as the master of our entire program. The submit job assigns all the necessary variables for the program to run such as file paths, number of mappers, user defined map and reduce functions. It also starts a new messenger for creating a communication channel between mappers, reducers and the master. It keeps track of all the mappers and reducers using the manage mappers and manage reducers methods respectively. Manage mappers does the work of connecting to the communication channel, creating partitions according to the number of mappers, creating a process for every mapper and assigning the relevant partition to each mapper. It waits for the multiprocessing to be completed and takes care of fault tolerance in the mappers. Once all the mapper processes have been completed, the main submit job function calls on the manage reducer method. Similar to the manage mapper method, manage reducers collects the paths of the intermediate files written by the mappers, assigns the relevant reducers their partitions and starts a process for each reducer. It also waits for all the processes to finish completing and takes care of fault tolerance. Lastly it deletes all the intermediate files once all the reducer processes have been completed. Once the entire map and reduce program is done, the submit job prints out that map reduce has been completed and gives us the path for the output files.




**Testing**

*src/tests/udfn* :

Where n is {1,2,3} for our three test cases. Test case 1 gives the count of each word as output given a text file. Test case 2 gives the unique words from the file. Test case 3 looks if the word specified by the user is present in the file. Each folder has an input and output folder. The input folder contains the file we are passing for the test case and the output folder has the final map-reduce output file. There is a config.properties file which contains the file path and the number of mappers information provided by the user. This folder also has three user defined java classes. “udfnMapper.java” has the user defined mapper function. “udfnReducer.java” has the user defined reducer function. The “udftestcasen.java”  creates a new instance of the mapreduce and initializes the job with the config.properties file path.




**How to Run?**

**You can run the code by running the run script that runs all of our test cases.**






