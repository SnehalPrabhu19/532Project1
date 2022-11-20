package src.tests.udf1;
import src.mapreduce.*;

import java.util.List;

public class udf1Reducer{
    public Pair<String, Integer> reduce(Pair input) {
        List<Integer> values = (List<Integer>) input.getValue();
        String key = (String) input.getKey();
        int result = 0;
        for (Integer v : values) {
            result += v;
        }

        return new Pair<String, Integer>(key, result);
    }
}
