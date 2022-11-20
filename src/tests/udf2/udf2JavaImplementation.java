package src.tests.udf2;

import src.mapreduce.Pair;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class udf2JavaImplementation {
    public static Map<String,Integer> test (String file) throws FileNotFoundException {
//        String file=args[0];

        Map<String,Integer> map=new HashMap<String,Integer>();
        Map<String,Integer> map2=new HashMap<String,Integer>();
        File demofile= new File(file);
        Scanner reader = new Scanner(demofile);
//        Boolean removePunctuations = true;
        String IPADDRESS_PATTERN =
                "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

        Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);

        while (reader.hasNextLine())
        {
            String line=reader.nextLine();
            String[] words=line.split(" ");
            for (String s : words) {
                String word = s.trim();
                Matcher matcher = pattern.matcher(word);
                if (matcher.find()) {
                    String newWord = matcher.group();
//                    int val = 1;
                    if (map.containsKey(newWord)) {
                        int val = 1 + map.get(newWord);
                        map.put(newWord, val);
                    } else {
                        map.put(newWord, 1);
                    }
                }
            }
        }
//        System.out.println(map);
        for (String k: map.keySet())
        {
            int val=map.get(k);
            if (val==1)
            {
                map2.put(k, val);
            }
        }
        reader.close();
//        System.out.println(map2);

        return map2;
    }
}
