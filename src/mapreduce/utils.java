package src.mapreduce;

//import com.sun.istack.internal.NotNull;
import java.io.*;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Stream;

public class utils {
    // Class to have all the static routines that can be used by all the instances of MapReduce
    // Routines for file handling

    public static String getFileExtension(String fileName) {
        // Get the extension of the input data file
        if (fileName == null) {
            throw new IllegalArgumentException("fileName must not be null!");
        }

        String extension = "";
        int index = fileName.lastIndexOf('.');
        if (index > 0) {
            extension = fileName.substring(index + 1);
        }

        return extension;

    }

    public static void deleteDir(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                if (! Files.isSymbolicLink(f.toPath())) {
                    deleteDir(f);
                }
            }
        }
        file.delete();
    }

    public static void createDirectory(String Path) {
        File dir = new File(Path);
        if (dir.exists()) {
            // System.out.printf("[WARN] Output directory already exists. Deleting and recreating: %s%n", Path);
            deleteDir(dir);
        }
        dir.mkdirs();
    }

    public static void writeOutputData(String temp_filePath, List<Pair<String, Integer>> list) {
        // Write data to the intermediate file
        try {
            File myObj = new File(temp_filePath);
            myObj.createNewFile();
            BufferedWriter bf = new BufferedWriter(new FileWriter(temp_filePath));

            for (Pair<String, Integer> p : list) {

                // put key and value separated by a colon
                String a = "(" + p.getKey() + ":" + String.valueOf(p.getValue()) + ")";
                bf.write(a);

                // new line
                bf.newLine();
            }
            bf.flush();
            try {
                // always close the writer
                bf.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            // System.out.println("[INFO] Successfully wrote to the file: "+ temp_filePath);
        } catch (IOException e) {
            System.err.println("[ERROR] An error occurred in writing data to file");
            e.printStackTrace();
        }
    }

    public static void writeTempData(Pair p, int numberOfMappers, String tempDirPath) {
        String keyValue = p.getKey() + " " + p.getValue();
        // Hashing based on key of the pair
        // Based on the integer hash value, we decide the intermediate file where it should go
        String fileName = String.valueOf(((Math.abs(p.getKey().hashCode())) % numberOfMappers) + 1);

        try {
            FileWriter fw = new FileWriter(tempDirPath + "/file_" + (fileName) + ".txt", true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(keyValue);
            bw.newLine();
            bw.close();
        } catch (IOException e) {
            System.err.println("[ERROR] An error occurred in writing intermediate data to disc.");
            e.printStackTrace();
        }
    }

    public static List<Pair<String, Integer>> readTempData(String tempFilePath) {
        // Reads temp data for reducers
        List<Pair<String, Integer>> finalOutput = new ArrayList<Pair<String, Integer>>();

        BufferedReader br = null;
        try {
            // create file object
            File file = new File(tempFilePath);

            // create BufferedReader object from the File
            br = new BufferedReader(new FileReader(file));

            String line = null;

            // read file line by line
            while ((line = br.readLine()) != null) {

                // split the line by :
                String[] parts = line.split(" ");

                // first part is name, second is number
                String name = parts[0].trim();
                String number = parts[1].trim();

                // put name, number in HashMap if they are
                // not empty
                if (!name.equals("") && !number.equals("")) {
                    Pair<String, Integer> addPair = new Pair<>(name, Integer.parseInt(number));
                    finalOutput.add(addPair);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            // Always close the BufferedReader
            if (br != null) {
                try {
                    br.close();
                } catch (Exception ignored) {
                }
                ;
            }
        }
        return finalOutput;
    }

    public static void createTempFiles(String temp_dir, int partitionCount) {
        for (int partitionID = 0; partitionID < partitionCount; partitionID++) {
            try {
                File myObj = new File(temp_dir + "/file_" + (partitionID + 1) + ".txt");
                if (myObj.createNewFile()) {
//                    System.out.println("[INFO] Temp Files created: " + myObj.getName());
                } else {
//                    System.out.println("[WARN] Temp File already exists. Deleting and recreating them");
                    myObj.delete();
                    myObj.createNewFile();
                }
            } catch (IOException e) {
                System.err.println("[ERROR] An error occurred in creating temp files");
                e.printStackTrace();
            }
        }
    }

    public static String readThisOffset(String inputFilePath, int offsetStartPos, int offsetEndPos, Boolean removePunctuations) {
        String outputData = "";
        int offsetLength = offsetEndPos - offsetStartPos;
//        System.out.println(removePunctuations);
        for (int i = 0; i < offsetLength; i++) {
            try (Stream<String> data = Files.lines(Paths.get(inputFilePath))) {
                outputData += data.skip(offsetStartPos + i).findFirst().get() + " ";
                outputData = removePunctuations ? outputData.replaceAll("\\p{Punct}","").toLowerCase() : outputData.toLowerCase();
            } catch (IOException e) {
                System.err.println(e);
            }
        }
        return outputData.trim();
    }

    public static ProcessBuilder execute(Class inputClass, List<String> args) {
        // Initialize a new process of inputClass
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classPath = System.getProperty("java.class.path");
        String className = inputClass.getName();

        List<String> command = new ArrayList<>();
        command.add(javaBin);
        command.add("-cp");
        command.add(classPath);
        command.add(className);
        command.addAll(args);

        // Build the process
        ProcessBuilder builder = new ProcessBuilder(command);
        return builder.inheritIO();
    }

    public static Class getClassName(String UDFclass) {
        Class cls = null;
        try {
            cls = Class.forName(UDFclass);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return cls;
    }

    public static Method getMethod(String methodName, Class cls) {
        Class[] partypes = new Class[1];
        partypes[0] = Pair.class;
        Method meth = null;
        try {
            meth = cls.getMethod(
                    methodName, partypes);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        return meth;
    }

    public static List<List<Integer>> readFileCreateOffsets(String inputFilePath, int no_of_mapper) throws IOException, FileNotFoundException {
        List<List<Integer>> mapperOffsets = new ArrayList<>();
        try {
            int LineCount = 0;

            // Read all input files and get the total line count
            File file = new File(inputFilePath);
            String fileExt = getFileExtension(file.getName());

            if (fileExt.contains("txt")) {
                // Read text files
//                System.out.println("[INFO] Reading text file");
                Scanner read = new Scanner(file);

                while (read.hasNextLine()) {
                    LineCount += 1;
                    read.nextLine();
                }
            }


            // Create offsets
            int partitionLength = LineCount / no_of_mapper;
//            System.out.printf("[INFO] Total lines found: %d | partitionLength: %d%n", LineCount, partitionLength);

            // Assuming no_of_reducers = no_of_mappers
            int start_pos = 0;
            int end_pos = 0;

            // Create (no_of_mapper * no_of_reducer) number of partitions
            for (int i = 1; i <= no_of_mapper; i++) {
                if (i == no_of_mapper) {
                    end_pos = LineCount;
                }
                else{
                    end_pos = start_pos + partitionLength;
                }
                List<Integer> thisOffset = new ArrayList<>();
                thisOffset.add(start_pos);
                thisOffset.add(end_pos);
                mapperOffsets.add(thisOffset);
                start_pos = end_pos;
            }
        } catch (FileNotFoundException e) {
            System.err.println("[ERROR] User input files not found at " + inputFilePath);
            e.printStackTrace();
        }

        return mapperOffsets;
    }
}