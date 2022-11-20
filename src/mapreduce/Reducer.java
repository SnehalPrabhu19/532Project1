package src.mapreduce;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.*;
import java.util.*;
import java.util.stream.Collectors;

import static src.mapreduce.messenger.createDataStream;
import static src.mapreduce.utils.*;

public class Reducer<K, V> {
    /*
    * Class for each reducer process in multiprocessing
    * */

    public static List<Pair<String, List<Integer>>> sortShuffle(List<Pair<String, Integer>> reducerInput) {
        /*
        * Method to aggregate the values at key level, then sort the output by key
        * @params:
        *   reducerInput: formatted input to each reducer from the intermediate files
        * @output:
        *   sorted list of <key, list of values> pairs
        * */

        // Aggregate the list of pair at pair.getKey level
        Map<String, List<Integer>> result1;
        result1 = reducerInput.stream()
                .collect(Collectors.groupingBy(Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toList())));

        // Sort the aggregated pairs at key level
        Map<String, List<Integer>> treeMap = new TreeMap<>(result1);
        // System.out.println("[INFO] [Reducer " + reducerID + "] After sorting: " + treeMap);

        // Convert Treemap back to List<Pair>
        List<Pair<String, List<Integer>>> finalReducerInput = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry : treeMap.entrySet())
            finalReducerInput.add(new Pair(entry.getKey(), entry.getValue()));

        return finalReducerInput;
    }

    public static void main(String[] args) throws InstantiationException, IllegalAccessException, InvocationTargetException, IOException {
        // BELOW TASKS TO BE PERFORMED BY EACH REDUCER

        // Connect to the messenger
        Pair<DataInputStream, DataOutputStream> dataStreams = createDataStream("reducer");
        DataInputStream inStream = dataStreams.getKey();
        DataOutputStream outStream = dataStreams.getValue();

        // Read common arguments from the messenger
        String UDFreducerPath = inStream.readUTF();
        String outputFilePath = inStream.readUTF();

        // Read instance specific arguments
        String tempFilePaths = args[0];
        int reducerID = Integer.parseInt(args[1]);
        int faultFlag = Integer.parseInt(args[2]);
        // System.out.printf("[INFO] [Reducer %d] tempFilePaths: %s%n", reducerID, tempFilePaths);

        // Convert List cast as string back to list
        List<String> directories = Arrays.asList(tempFilePaths.split(" "));
        if (faultFlag==1 && reducerID==1){
            // outStream.writeUTF(tempDirPath);
            System.err.printf("[ERROR] [Reducer %d] Error in Reducer. Exiting...%n", reducerID);
            System.exit(130);
        }

        // Read data from relevant partition of temp data
        List<Pair<String, Integer>> reducerInput = new ArrayList<>();
        String outputFileName = "file_" + reducerID + ".txt";
        for (String dir : directories) {
            String inputFilePath = dir + outputFileName;
            // System.out.printf("[INFO] [Reducer %d] Reading input keys from: %s%n", reducerID, inputFilePath);
            reducerInput.addAll(readTempData(inputFilePath));
        }

        // Sort and shuffle the intermediate data
        List<Pair<String, List<Integer>>> finalReducerInput = sortShuffle(reducerInput);

        // Get class name and method for Java Reflections
        Class cls = getClassName(UDFreducerPath);
        Method meth = getMethod("reduce", cls);

        List<Pair<String, Integer>> outputList = new ArrayList<>();
        for (Pair<String, List<Integer>> p : finalReducerInput) {
            Object obj = cls.newInstance();
            // Invoke UDF using JAVA Reflection
            Object result = meth.invoke(obj, p);

            Pair<String, Integer> outputPair = (Pair<String, Integer>) result;
            if (outputPair.getKey() != null && outputPair.getValue() != null)
                outputList.add(outputPair);
        }

        // Write output data to file
        String outFile = outputFilePath + "/" + "file_" + reducerID + ".txt";
        writeOutputData(outFile, outputList);

        // Send message with output file path to messenger
        outStream.writeUTF(outFile);

        // System.out.printf("[Reducer %d] OutFile: %s | OutputList: %s%n", reducerID, outFile, Arrays.toString(outputList.toArray()));
        // System.out.printf("[INFO] [Reducer %d] Successfully completed reduce operations!%n", reducerID);
    }
}
