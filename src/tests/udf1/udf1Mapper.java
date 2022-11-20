package src.tests.udf1;

import src.mapreduce.*;

import java.util.ArrayList;
import java.util.List;

public class udf1Mapper {

    public List<Pair<String, Integer>> map(Pair input) {
        String lines = String.valueOf(input.getKey());
        String[] words = lines.split(" ");
        // System.out.println("WORDS " + words);
        List<Pair<String, Integer>> output = new ArrayList<>();
        for (String word : words) {
            // System.out.println(words[k].toLowerCase() + " | " + 1);
            output.add(new Pair<String, Integer>(word.toLowerCase(), 1));
        }
        return output;
    }
}
