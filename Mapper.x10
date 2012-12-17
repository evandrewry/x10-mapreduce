import x10.util.Pair;
import x10.util.List;

/*
* this class should be subclassed with the map function
* implemented, and then the subclass should be passed into
* a MapReduceJob
*/
public abstract class Mapper[IK, IV, OK, OV] {
    /* implement this method with a custom map function */
    public abstract def map(key:IK, 
        value:IV,
        outputCollector:MapperOutputCollector[OK, OV])
    :void;

    /* will map input and partition results according to the map
    * and partition functions. This should not be overridden. */
    public def run(input:List[Pair[IK,IV]],
        partitions:Int,
        partition_op:(OK, Int)=>Int)
    {
        val oc = new MapperOutputCollector[OK, OV](partitions, partition_op);
        for (i in input)
            map(i.first, i.second, oc);
        return oc.get();
    }

}
