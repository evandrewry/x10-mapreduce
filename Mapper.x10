import x10.util.HashMap;
import x10.util.List;

public abstract class Mapper[IK, IV, OK, OV] {
    public abstract def map(key:IK, 
        value:IV,
        outputCollector:OutputCollector[OK, OV])
    :void;

    public def run(input:HashMap[IK,IV],
        outputCollector:OutputCollector[OK, OV])
    :HashMap[OK, OV] {
        for (i in input.keySet()) map(i, input.get(i).value, outputCollector);
        return outputCollector.get();
    }

}
