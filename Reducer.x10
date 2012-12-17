import x10.util.HashMap;
import x10.util.List;

/*
* this class should be subclassed with the reduce method
* implemented, and then the subclass should be passed into
* the MapReduceJob constructor in order to use it in a job
*/
public abstract class Reducer[IK, IV, OK, OV] {

    /* implement this method in a subclass with a custom reduce function */
    public abstract def reduce(key:IK,
                               values:List[IV],
                               outputCollector:ReducerOutputCollector[OK, OV])
    :void;


    /* iterates through input and reduces each, collecting results in
    * an OutputColector object. This should not be overridden. */
    public def run(input:HashMap[IK, List[IV]])
    {
        val oc = new ReducerOutputCollector[OK, OV]();
        for (k in input.keySet())
                reduce(k, input.get(k).value, oc);
        return oc.get();
    }

}
