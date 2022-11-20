package src.unitTest;

//import org.junit.Test;
import src.tests.udf1.udf1JavaImplementation;
import src.tests.udf1.udftestcase1;
import src.tests.udf2.udf2JavaImplementation;
import src.tests.udf2.udftestcase2;
import src.tests.udf3.udf3JavaImplementation;
import src.tests.udf3.udftestcase3;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class automatedTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        testUDF1();
        testUDF2();
        testUDF3();
        testFaultTolerance();
    }

    public static void testUDF1() throws IOException, InterruptedException {
        System.out.println("---------Testing UDF 1-------------");
        // Run the test case UDF using mapreduce
        System.out.println("[INFO] Running UDF1 using Map Reduce");
        String[] args = {"./src/tests/udf1/config.properties"};
        udftestcase1.main(args);

        System.out.println("[INFO] Running UDF1 using JAVA");
        Map<String,Integer> out1 = udf1JavaImplementation.test("./src/tests/udf1/input/hamlet.txt");

        // Compare the output of UDF1 with predefined output
        System.out.println("[INFO] Comparing the output of Map Reduce with Java");

        Map<String,Integer> map_udf1 = new HashMap<>();
        File outputDir = new File("./src/tests/udf1/output/");
        List<File> outputFiles = new ArrayList<>();
        Collections.addAll(outputFiles, Objects.requireNonNull(outputDir.listFiles()));
        for (File outputFile : outputFiles) {
            Scanner read = new Scanner(outputFile);
            while (read.hasNextLine()) {
                String line = read.nextLine();
                line = line.replaceAll("[()]*", "");
                String word = line.split(":")[0];
//                System.out.println(line + " | " + word + ": ");
//                if (line.length() != 0) {
                int count = Integer.parseInt(line.split(":")[1]);
                map_udf1.put(word, count);
//                }
            }
        }
        if (map_udf1.equals(out1)) {
            System.out.println("[SUCCESS] Test Case for UDF1 Passed!");
        }
        else
            System.err.println("[FAILED] Test Case for UDF1 Failed!");
        assert map_udf1.equals(out1);
    }

    public static void testUDF2() throws IOException, InterruptedException {
        // Run the test case UDF
        System.out.println("---------Testing UDF 2-------------");
        System.out.println("[INFO] Running UDF2 using Map Reduce");
        String[] args = {"./src/tests/udf2/config.properties"};
        udftestcase2.main(args);

        System.out.println("[INFO] Running UDF2 using JAVA");
        Map<String, Integer> out2 = udf2JavaImplementation.test("./src/tests/udf2/input/linuxLog.txt");

        // Compare the output of UDF2 with predefined output
        System.out.println("[INFO] Comparing the output of Map Reduce with Java");

        Map<String, Integer> map_udf2 = new HashMap<String, Integer>();
        File outputDir = new File("./src/tests/udf2/output/");
        List<File> outputFiles = new ArrayList<>();
        Collections.addAll(outputFiles, Objects.requireNonNull(outputDir.listFiles()));
        for (File outputFile : outputFiles) {
            Scanner read2 = new Scanner(outputFile);
            while (read2.hasNextLine()) {
                String line = read2.nextLine();
                line = line.replaceAll("[()]*", "");
                String word = line.split(":")[0];
                int count = Integer.parseInt(line.split(":")[1]);
                map_udf2.put(word, count);
            }
        }
//        System.out.println(map_udf2);
//        System.out.println(out2);
        if (map_udf2.equals(out2)) {
            System.out.println("[SUCCESS] Test Case for UDF2 Passed!");
        }
        else
            System.err.println("[FAILED] Test Case for UDF2 Failed!");
        assert map_udf2.equals(out2);
    }

    public static void testUDF3() throws IOException, InterruptedException {
        // Run the test case UDF
        System.out.println("---------Testing UDF 3-------------");
        System.out.println("[INFO] Running UDF3 using Map Reduce");
        String[] args = {"./src/tests/udf3/config.properties"};
        udftestcase3.main(args);

        System.out.println("[INFO] Running UDF3 using JAVA");
        Map<String, Integer> out3 = udf3JavaImplementation.test("./src/tests/udf3/input/macLog.txt");

        // Compare the output of UDF3 with predefined output
        System.out.println("[INFO] Comparing the output of Map Reduce with Java");

        Map<String,Integer> map_udf3=new HashMap<String,Integer>();
        File outputDir = new File("./src/tests/udf3/output/");
        List<File> outputFiles = new ArrayList<>();
        Collections.addAll(outputFiles, Objects.requireNonNull(outputDir.listFiles()));
        for (File outputFile : outputFiles) {
            Scanner read3 = new Scanner(outputFile);
            while (read3.hasNextLine())
            {
                String line=read3.nextLine();
                line = line.replaceAll("[()]*", "");
                String word=line.split(":")[0];
                int count=Integer.parseInt(line.split(":")[1]);
                map_udf3.put(word, count);
            }
        }
        if (map_udf3.equals(out3)) {
            System.out.println("[SUCCESS] Test Case for UDF3 Passed!");
        }
        else
            System.err.println("[FAILED] Test Case for UDF3 Failed!");
        assert map_udf3.equals(out3);

    }

    public static void testFaultTolerance() throws IOException, InterruptedException {
        // Run the test case UDF
        try{
            System.out.println("---------Testing 1 occurrence of fault tolerance-------------");

            // Run any one UDF and kill the process
            System.out.println("[INFO] Checking Fault Tolerance using UDF 1");
            System.out.println("[INFO] Running UDF1 using Map Reduce");
            String[] args = {"./src/tests/udf1/configFault.properties", "1"};
            udftestcase1.main(args);

            // Generate the output of the same UDF using JAVA
            System.out.println("[INFO] Running UDF1 using JAVA");
            Map<String,Integer> out1 = udf1JavaImplementation.test("./src/tests/udf1/input/hamlet.txt");

            // Compare the output of Map Reduce and Java
            System.out.println("[INFO] Comparing the output of Map Reduce with Java");

            Map<String,Integer> map_udf1 = new HashMap<>();
            File outputDir = new File("./src/tests/udf1/output/");
            List<File> outputFiles = new ArrayList<>();
            Collections.addAll(outputFiles, Objects.requireNonNull(outputDir.listFiles()));
            for (File outputFile : outputFiles) {
                Scanner read = new Scanner(outputFile);
                while (read.hasNextLine()) {
                    String line = read.nextLine();
                    line = line.replaceAll("[()]*", "");
                    String word = line.split(":")[0];
                    if (line.length() != 0) {
                        int count = Integer.parseInt(line.split(":")[1]);
                        map_udf1.put(word, count);
                    }
                }
            }
            if (map_udf1.equals(out1)) {
                System.out.println("[SUCCESS] Test Case for Fault Tolerance Passed!");
            }
            else
                System.err.println("[FAILED] Test Case for Fault Tolerance Failed!");
            assert map_udf1.equals(out1);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }


}