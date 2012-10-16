import x10.util.HashMap;

public class OutputCollector[T, U] {
    val output = new HashMap[T, U]();  
    def collect(key:T, value:U) {
        output.put(key, value);
    }
    def get()
    :HashMap[T,U] {
        return output;
    }
}
