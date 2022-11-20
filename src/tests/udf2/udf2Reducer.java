package src.tests.udf2;
import src.mapreduce.*;

import java.util.List;

public class udf2Reducer{
    public Pair<String, Integer> reduce(Pair input) {
        List<Integer> values = (List<Integer>) input.getValue();
        String key = (String) input.getKey();
        int result = 0;
        for (Integer v : values) {
            result += v;
        }
        if (result == 1) {
            return new Pair<String, Integer>(key, result);
        }
//            return values;
        return new Pair<String, Integer>(null, null);
    }
}
