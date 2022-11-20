package src.mapreduce;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.*;
import java.util.List;

import static src.mapreduce.messenger.createDataStream;
import static src.mapreduce.utils.*;


public class Mapper<K, V> {
    /*
     * Class for each reducer process in multiprocessing
     * */


    public static void main(String[] args) throws Exception{
        // BELOW TASKS TO BE PERFORMED BY EACH REDUCER

        // Connect to the messenger
        Pair<DataInputStream, DataOutputStream> dataStreams = createDataStream("mapper");
        DataInputStream inStream = dataStreams.getKey();
        DataOutputStream outStream = dataStreams.getValue();

        // Read common arguments from the messenger
        String inputFilePath = inStream.readUTF();
        String UDFmapperPath = inStream.readUTF();
        int no_of_mappers = Integer.parseInt(inStream.readUTF());
        String removePunct = inStream.readUTF();
        Boolean removePunctuations = false;
        if (removePunct.equals("yes")) {
            removePunctuations = true;
        }

        // Read instance specific arguments
        int offsetStartPos = Integer.parseInt(args[0]);
        int offsetEndPos = Integer.parseInt(args[1]);
        int mapperID = Integer.parseInt(args[2]);
        int faultFlag = Integer.parseInt(args[3]);

        // Only mapperID 1 will fail if faultFlag = 1
        if (faultFlag==1 && mapperID==1){
            System.err.printf("[ERROR] [MAPPER %d] Error in mapper. Exiting...%n", mapperID);
            System.exit(130);
        }

        // Create dir for intermediate files
        String tempDirPath = "./temp/"+mapperID+"/";
        createDirectory(tempDirPath);
        createTempFiles(tempDirPath, no_of_mappers);

        // Read data from input files with offsets and convert it to key-value pair
        String mapperRawInput = readThisOffset(inputFilePath, offsetStartPos, offsetEndPos, removePunctuations);
        Pair<String, Integer> mapperInput = new Pair<>(mapperRawInput, null);

        // Get class name and method for Java Reflections
        Class cls = getClassName(UDFmapperPath);
        Method meth = getMethod("map", cls);

        // Invoke UDF using JAVA Reflection
        Object obj = cls.newInstance();
        Object res = meth.invoke(obj, mapperInput);
        List<Pair> list = (List<Pair>) res;
        for(Pair p: list) {
            // Write temp data based on hashing to selected partitions
            writeTempData(p, no_of_mappers, tempDirPath);
        }

        // Send message with intermediate file path to messenger
        outStream.writeUTF(tempDirPath);

        // System.out.printf("[INFO] [Mapper %d] Successfully completed map operations!%n", mapperID);
        System.exit(0);
    }

}
