import x10.util.HashMap;
import x10.util.List;
import x10.util.Timer;
import x10.util.ArrayList;

public class MapReduceJob[IK, IV, CK, CV, OK, OV] {
    private static val timer = new Timer();
    private val mapper:Mapper[IK, IV, CK, CV];
    private val reducer:Reducer[CK, CV, OK, OV];
    private val m_output_collector:OutputCollector[CK, CV];
    private val r_output_collector:OutputCollector[OK, OV];    
    private val partition_op:(CK, Int) => Int;

    public def this(mapper:Mapper[IK, IV, CK, CV],
                    reducer:Reducer[CK, CV, OK, OV]) {
        this.mapper = mapper;
        this.reducer = reducer;
        this.m_output_collector = new OutputCollector[CK, CV]();
        this.r_output_collector = new OutputCollector[OK, OV]();
        this.partition_op = (k:CK, n:Int) => Math.abs(k.hashCode()) % n;

    }

    public def this(mapper:Mapper[IK, IV, CK, CV],
                    reducer:Reducer[CK, CV, OK, OV],
                    m_output_collector:OutputCollector[CK, CV],
                    r_output_collector:OutputCollector[OK, OV],
                    partition_op:(CK, Int)=>Int) {
        this.mapper = mapper;
        this.reducer = reducer;
        this.m_output_collector = m_output_collector;
        this.r_output_collector = r_output_collector;
        this.partition_op = partition_op;
    }


    public def run(input:List[HashMap[IK, IV]])
    :HashMap[OK, OV] {
        val reducers = input.size();
        var start:Long = timer.nanoTime();
        val intermediates = new Rail[HashMap[CK, CV]](reducers);
        finish for (i in 0..(input.size() - 1)) async {
            intermediates(i) = mapper.run(input(i), m_output_collector.make());
        }
        Console.OUT.println("map!" + timer.nanoTime() - start);

        /* shuffle */
        val shuffled = new Rail[HashMap[CK, List[CV]]](reducers, (i:Int)=>new HashMap[CK, List[CV]]());
        /*iterate over intermediate results */

        start = timer.nanoTime();
        for (i in intermediates) {
            /* iterate over keys for each intermediates result */
            for (k in intermediates(i).keySet()) {
                val v = intermediates(i).get(k).value;
                /* calculate reduce partition for this key */
                val part = partition_op(k, reducers);
                val partition = shuffled(part);
                if (partition.containsKey(k)) {
                    partition.get(k).value.add(v);
                } else {
                    val list = new ArrayList[CV]();
                    list.add(v);
                    partition.put(k, list as List[CV]);
                }
            }
        }
        Console.OUT.println("partition!" + timer.nanoTime() - start);

        val reduced = new Rail[HashMap[OK, OV]](reducers);
        start = timer.nanoTime();
        finish for (i in 0..(reducers - 1)) async {
            reduced(i) = reducer.run(shuffled(i), r_output_collector.make());
        }
        Console.OUT.println("reduce!" + timer.nanoTime() - start);

        val output = new HashMap[OK, OV]();
        start = timer.nanoTime();
        for (i in reduced) {
            for (k in reduced(i).keySet()) {
                output.put(k, reduced(i).get(k).value);
            }
        }
        Console.OUT.println("reduce more!" + timer.nanoTime() - start);

        return output;

    }

}
