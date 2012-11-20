import x10.util.HashMap;
import x10.util.List;
import x10.util.ArrayList;

public class MapReduceJob[IK, IV, CK, CV, OK, OV] {
    private val mapper:Mapper[IK, IV, CK, CV];
    private val reducer:Reducer[CK, CV, OK, OV];
    private val m_output_collector:OutputCollector[CK, CV];
    private val r_output_collector:OutputCollector[OK, OV];    

    public def this(mapper:Mapper[IK, IV, CK, CV],
                    reducer:Reducer[CK, CV, OK, OV]) {
        this.mapper = mapper;
        this.reducer = reducer;
        this.m_output_collector = new OutputCollector[CK, CV]();
        this.r_output_collector = new OutputCollector[OK, OV]();

    }

    public def this(mapper:Mapper[IK, IV, CK, CV],
                    reducer:Reducer[CK, CV, OK, OV],
                    m_output_collector:OutputCollector[CK, CV],
                    r_output_collector:OutputCollector[OK, OV]) {
        this.mapper = mapper;
        this.reducer = reducer;
        this.m_output_collector = m_output_collector;
        this.r_output_collector = r_output_collector;
    }


    public def run(input:List[HashMap[IK, IV]])
    :HashMap[OK, OV] {
        val reducers = input.size();
        val intermediates = new Rail[HashMap[CK, CV]](reducers);
        finish for (i in 0..(input.size() - 1)) async {
            intermediates(i) = mapper.run(input(i), m_output_collector.make());
        }

        val shuffled = new Rail[HashMap[CK, List[CV]]](reducers, new HashMap[CK, List[CV]]());
        for (i in intermediates) {
            for (k in intermediates(i).keySet()) {
                val v = intermediates(i).get(k).value;
                val part = partition(k, reducers);
                if (shuffled(part).containsKey(k)) {
                    shuffled(part).get(k).value.add(v);
                } else {
                    val list = new ArrayList[CV]();
                    list.add(v);
                    shuffled(part).put(k, list as List[CV]);
                }
            }
        }

        val reduced = new Rail[HashMap[OK, OV]](reducers);
        finish for (i in 0..(shuffled.size - 1)) async {
            reduced(i) = reducer.run(shuffled(i), r_output_collector.make());
        }

        val output = new HashMap[OK, OV]();
        for (i in reduced) {
            for (k in reduced(i).keySet()) {
                output.put(k, reduced(i).get(k).value);
            }
        }

        return output;

    }

    private def partition(x:CK, reducers:Int)
    :Int {
        return Math.abs(x.hashCode()) % reducers;
    }
}
