import x10.util.HashMap;
import x10.util.Box;

public class OutputCollector[T, U] {
    var output:HashMap[T, U];

    def collect(key:T, value:U) {
        put(key, value);
    }

    def get(key:T) {
        return output.get(key);
    }

    def put(key:T, value:U) {
        output.put(key, value);
    }

    def get()
    :HashMap[T,U] {
        return output;
    }

    def make() {
        return new OutputCollector[T, U]();
    }
    
    def reset() {
        output = new HashMap[T, U]();
    }
}
