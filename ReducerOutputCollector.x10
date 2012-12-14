import x10.util.ArrayList;
import x10.util.Pair;

public class ReducerOutputCollector[T, U] {
    val output:ArrayList[Pair[T, U]];

    def this() {
      output = new ArrayList[Pair[T, U]]();
    }

    def collect(key:T, value:U) {
        output.add(Pair[T, U](key, value));
    }

    def get() {
        return output;
    }
}
