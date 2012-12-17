import x10.util.ArrayList;
import x10.util.Pair;

/* collects the results of a single Reduce worker. */
public class ReducerOutputCollector[T, U] {
    /* data structure for storing output */
    val output:ArrayList[Pair[T, U]];

    def this() {
      output = new ArrayList[Pair[T, U]]();
    }

    /* adds a single key, value result to the result set */
    def collect(key:T, value:U) {
        output.add(Pair[T, U](key, value));
    }

    /* returns result set */
    def get() {
        return output;
    }
}
