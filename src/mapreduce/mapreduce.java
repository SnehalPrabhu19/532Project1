/*
* Java Package to implement the functionality of Map Reduce
* Author: Rishabh Garodia, Snehal Prabhu, Shruti Shelke
* */

package src.mapreduce;

import java.io.*;
import java.util.*;

import static src.mapreduce.faultTolerance.mapperFaultTolerance;
import static src.mapreduce.faultTolerance.reducerFaultTolerance;
import static src.mapreduce.utils.*;

public class mapreduce {
	private static messenger ms;

	public void SubmitJob(Properties properties) throws FileNotFoundException, IOException, InterruptedException {
		/* Main routine that triggers the Map Reduce.
		* @params:
		* 	properties: Java .properties file provided by the user.
		*
		* Loads all the properties and then starts the mappers.
		* Waits for the mappers to complete.
		* Then starts the reducers.
		* Finally, prints the output path of the results for the user.
		*/

		//Read input arguments from properties file
		Properties advProperties = new Properties();

		try (InputStream input = new FileInputStream("./src/resource/advancedConfig.properties")) {
			// load a properties file
			advProperties.load(input);
		} catch (IOException ex) {
			ex.printStackTrace();
		}


		String removePunctuations = advProperties.getProperty("removePunctuations");
		String faultFlag = advProperties.getProperty("faultFlag");
		Integer no_of_mapper = Integer.parseInt(properties.getProperty("numberOfMappers"));
		String inputFilePath = properties.getProperty("inputFilePath");
		String outputFilePath = properties.getProperty("outputFilePath");
		String UDFmapperPath = properties.getProperty("udfClassMapper");
		String UDFreducerPath = properties.getProperty("udfClassReducer");
		try {
			// If faultFlag == 1, mapper 1 and reducer 1 will fail due to error
			faultFlag = properties.getProperty("faultFlag");
			removePunctuations = properties.getProperty("removePunctuations", null);
//			System.out.println(removePunctuations);
			if (faultFlag == null) {
				faultFlag = "0";
			}
			if (removePunctuations == null) {
				removePunctuations = advProperties.getProperty("removePunctuations");
			}
		} catch (Exception e) {}

		 System.out.println("[INFO] Input file path: " + inputFilePath);
		 System.out.println("[INFO] Output file path: " + outputFilePath);
		 System.out.println("[INFO] Number of Mappers/Reducers: " + no_of_mapper);

		ms = new messenger();

		// Start distributed mappers
		ManageMappers(inputFilePath, no_of_mapper, UDFmapperPath, faultFlag, removePunctuations);
		// Start distributed mappers
		ManageReducer(outputFilePath, no_of_mapper, UDFreducerPath, faultFlag);
		// Get paths of intermediate files
		List<String> outputFilePaths = ms.getOutputFilePaths();
		System.out.printf("[INFO] MapReduce complete. Output created at: %n");
		for (String s : outputFilePaths) System.out.println(s);

		// Delete the temp files and folders and shut down the messenger sockets
		deleteDir(new File("./temp"));
		ms.shutConnections();
	}

	public void ManageMappers(String inputFilePath, int no_of_mapper, String UDFmapperPath, String faultFlag, String removePunctuations) throws FileNotFoundException, IOException {
		/*
		* Method to manage all the mappers
		* @params:
		* 	inputFilePath: file path of the input file provided by the user
		* 	no_of_mapper: number of mapper/reducers to be started
		* 	UDFmapperPath: class reference path of the UDF Mapper method provided by the user
		* 	faultFlag: Flag = 0 if not fault to be created, 1 if fault is to be created
		*
		* Method to create partitions for the mappers, start the mapper processes, manage communication with the mappers
		* */
		try {
			// Start socket for communicating with the mappers
			ms.connectMapper(inputFilePath, no_of_mapper, UDFmapperPath, removePunctuations);

			// Read inputFilePath and create offsets for each mapper
			List<List<Integer>> mapperOffsets = readFileCreateOffsets(inputFilePath, no_of_mapper);
			// System.out.println("[INFO] mapperOffsets " + mapperOffsets);

			// Loop over each offset to start a new mapper
			List<Process> mapperProcesses = new ArrayList<>();
			for (int mapperID = 1; mapperID <= no_of_mapper; mapperID ++) {
				// get the offset values for this mapper process
				List<Integer> mapperOffset = mapperOffsets.get(mapperID-1);
				// System.out.printf("[INFO] [Mapper %d] mapperOffset: %s%n", mapperID, Arrays.toString(mapperOffset.toArray()));
				// System.out.printf("[INFO] [Mapper %d] faultFlag: %s%n", mapperID, faultFlag);

				// Add offset start pos, end pos, mapper id and faultFlag to mapperArgs
				List<String> mapperArgs = new ArrayList<>();
				mapperArgs.add(String.valueOf(mapperOffset.get(0)));
				mapperArgs.add(String.valueOf(mapperOffset.get(1)));
				mapperArgs.add(String.valueOf(mapperID));
				mapperArgs.add(faultFlag);

				// Start multiprocessing
				// System.out.printf("[INFO] [Mapper %d] Starting Mapper%n", mapperID);
				mapperProcesses.add(execute(Mapper.class, mapperArgs).start());
				}

			// Wait for processes to complete
			for (int i = 0; i < no_of_mapper; i++) {
				mapperProcesses.get(i).waitFor();
			}

			// Handle fault tolerance
			mapperProcesses = mapperFaultTolerance(no_of_mapper, mapperProcesses, mapperOffsets);

			// Shut the mapper messenger thread
			ms.shutMapperThread();

		} catch (FileNotFoundException | InterruptedException e) {
			System.err.println("[ERROR] Input files not found for starting Mappers");
			e.printStackTrace();
		}
	}
	
	public void ManageReducer(String outputFilePath, int no_of_mapper, String UDFreducerPath, String faultFlag) throws IOException, InterruptedException {
		/*
		 * Method to manage all the reducers
		 * @params:
		 * 	outputFilePath: file path of the output file provided by the user
		 * 	no_of_mapper: number of mapper/reducers to be started
		 * 	UDFreducerPath: class reference path of the UDF Reducer method provided by the user
		 * 	faultFlag: Flag = 0 if not fault to be created, 1 if fault is to be created
		 *
		 * Method to start reducers and manage communication with them
		 * */

		// Get paths of intermediate files from messenger
		String tempFilePaths = ms.getTempFilePaths();

		// Connect messenger for reducers
		ms.connectReducer(outputFilePath, no_of_mapper, UDFreducerPath);

		//Create output directory
		createDirectory(outputFilePath);

		// Loop to start a new reducer
		List<Process> reducerProcesses = new ArrayList<>();
		for (int reducerID = 1; reducerID <= no_of_mapper; reducerID++) {
			// Create reducer Args
			List<String> reducerArgs = new ArrayList<>();
			reducerArgs.add(tempFilePaths);
			reducerArgs.add(String.valueOf(reducerID));
			reducerArgs.add(faultFlag);

			// Start multiprocessing
			// System.out.printf("[INFO] [Reducer %d] Starting Reducer%n", reducerID);
			reducerProcesses.add(execute(Reducer.class, reducerArgs).start());
		}

		// Wait for processes to complete
		for (int j = 0; j < no_of_mapper; j++) {
			reducerProcesses.get(j).waitFor();
		}

		// Handle fault tolerance
		reducerProcesses = reducerFaultTolerance(no_of_mapper, reducerProcesses, tempFilePaths);

		// Shut the reducer messenger thread
		ms.shutReducerThread();
	}
}
