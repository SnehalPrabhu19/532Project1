package src.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static src.mapreduce.utils.execute;

public class faultTolerance {
    public static List<Process> mapperFaultTolerance(Integer no_of_mapper, List<Process> mapperProcesses, List<List<Integer>> mapperOffsets) throws InterruptedException, IOException {
//        boolean restartFlag = false;
//        List<Integer> restartedIndex = new ArrayList<>();
        int counter=0;
        for (; counter < no_of_mapper; counter++) {
//            System.out.println(counter + " " + mapperProcesses.get(counter).exitValue());
            if (mapperProcesses.get(counter).exitValue() != 0) {
                System.out.printf("[INFO] Starting mapper ID %d again as it encountered some error%n", (counter+1));
                List<String> mapperArgs = new ArrayList<>();
                mapperArgs.add(String.valueOf(mapperOffsets.get(counter).get(0)));
                mapperArgs.add(String.valueOf(mapperOffsets.get(counter).get(1)));
                mapperArgs.add(String.valueOf(counter+1));
                mapperArgs.add(String.valueOf(0));

                mapperProcesses.add(counter, execute(Mapper.class, mapperArgs).start());
                break;
            }
        }
        // Wait for all the restarted processes
        if (counter < no_of_mapper)
//            for (Integer i : restartedIndex)
            mapperProcesses.get(counter).waitFor();
        return mapperProcesses;
    }

    public static List<Process> reducerFaultTolerance(Integer no_of_mapper, List<Process> reducerProcesses, String tempFilePaths) throws InterruptedException, IOException {
//        boolean restartFl /tartedIndex = new ArrayList<>();
        int counter = 0;
        for (; counter < no_of_mapper; counter++) {
            // Rerun process if exit status != 0
            if (reducerProcesses.get(counter).exitValue() != 0) {
                System.out.printf("[INFO] Starting reducer ID %d again as it encountered some error%n", (counter+1));
                List<String> reducerArgs = new ArrayList<>();
                reducerArgs.add(tempFilePaths);
                reducerArgs.add(String.valueOf(counter+1));
                reducerArgs.add(String.valueOf(0));

                reducerProcesses.add(counter, execute(Reducer.class, reducerArgs).start());
//                restartFlag = true;
//                restartedIndex.add(counter);
                break;
            }
        }
        // Wait for all the restarted processes
        if (counter < no_of_mapper)
//            for (Integer i : restartedIndex)
            reducerProcesses.get(counter).waitFor();
        return reducerProcesses;
    }

}
