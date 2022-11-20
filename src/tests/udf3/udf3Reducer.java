package src.tests.udf3;
import src.mapreduce.*;

import java.util.List;

public class udf3Reducer{
    public Pair<String, Integer> reduce(Pair input) {
        List<Integer> values = (List<Integer>) input.getValue();
        String key = (String) input.getKey();
        int result = 0;
        String word="error";
        if (key.equals(word))
        {
            for (Integer v : values)
            {
                result += v;
            }
            return new Pair<String, Integer>(key, result);
        }
        return new Pair<String, Integer>(null, null);
    }
}
