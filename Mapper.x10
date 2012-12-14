import x10.util.Pair;
import x10.util.List;

public abstract class Mapper[IK, IV, OK, OV] {
    public abstract def map(key:IK, 
        value:IV,
        outputCollector:MapperOutputCollector[OK, OV])
    :void;

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
