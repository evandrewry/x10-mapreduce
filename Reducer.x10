import x10.util.HashMap;
import x10.util.List;

public abstract class Reducer[IK, IV, OK, OV] {

    public abstract def reduce(key:IK,
           values:List[IV],
           outputCollector:OutputCollector[OK, OV])
    :void;

    public def run(input:HashMap[IK, List[IV]],
        outputCollector:OutputCollector[OK, OV])
    :HashMap[OK, OV] {
        for (k in input.keySet())
                reduce(k, input.get(k).value, outputCollector);
        return outputCollector.get();
    }

}
