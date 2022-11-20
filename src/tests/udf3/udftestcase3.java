package src.tests.udf3;

import src.mapreduce.mapreduce;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class udftestcase3 {
    // Count the number of all the words that are present only once in the text

    public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
        String configFilePath = args[0];

        mapreduce MR = new mapreduce();
        Properties properties = new Properties();

        try (InputStream input = new FileInputStream(configFilePath)) {
            // load a properties file
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        MR.SubmitJob(properties);

        // TODO Auto-generated method stub
    }
}