import x10.util.ArrayList;
import x10.util.Pair;

public class MapperOutputCollector[T, U] {
    private val output:Rail[ArrayList[Pair[T, U]]];
    private val partition_op:(T, Int) => Int;
    private val n:Int;

    def this(n:Int, partition_op:(T, Int)=>Int) {
        this.partition_op = partition_op;      
        this.n = n;
        this.output = new Rail[ArrayList[Pair[T,U]]](n,
            (i:Int) => new ArrayList[Pair[T,U]]());
    }

    def collect(key:T, value:U) {
        output(partition_op(key, n)).add(Pair[T, U](key, value));
    }

    def get()
    {
        return output;
    }
}
