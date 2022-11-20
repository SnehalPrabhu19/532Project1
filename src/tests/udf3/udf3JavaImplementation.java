package src.tests.udf3;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class udf3JavaImplementation {
    public static Map<String,Integer> test (String file) throws FileNotFoundException {
//        String file=args[0];

        Map<String,Integer> map=new HashMap<String,Integer>();
        Map<String,Integer> map3=new HashMap<String,Integer>();
        File demofile= new File(file);
        Scanner reader = new Scanner(demofile);
        Boolean removePunctuations = true;

        while (reader.hasNextLine())
        {
            String line=reader.nextLine();
            String[] words=line.split(" ");
            for(int k=0; k< words.length; k++)
            {
                String word = words[k].trim();
                word = removePunctuations ? word.replaceAll("\\p{Punct}", "").toLowerCase() : word.toLowerCase();
                if (word != null && !word.isEmpty()) {
//                    int val = 1;
                    if (map.containsKey(word)) {
                        int val = 1 + map.get(word);
                        map.put(word, val);
                    } else {
                        map.put(word, 1);
                    }
                }
            }
        }
        for (String k: map.keySet())
        {
            int val=map.get(k);
            if(k.equals("error"))
            {
                map3.put(k,val);
            }
        }
        reader.close();

        return map3;
    }
}
