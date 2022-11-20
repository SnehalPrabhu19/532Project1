package src.tests.udf2;

import src.mapreduce.*;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class udf2Mapper {

    public List<Pair<String, Integer>> map(Pair input) {
        String lines = String.valueOf(input.getKey());
//        System.out.println(lines);
        String[] words = lines.split(" ");
        // System.out.println("WORDS " + words);
        List<Pair<String, Integer>> output = new ArrayList<>();

        String IPADDRESS_PATTERN =
                "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

        Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
        for (String word : words) {
//            System.out.println(word);
            Matcher matcher = pattern.matcher(word.trim());
            if (matcher.find()) {
                String newWord = matcher.group();
                output.add(new Pair<String, Integer>(newWord, 1));
            }
        }
        return output;
    }
}
