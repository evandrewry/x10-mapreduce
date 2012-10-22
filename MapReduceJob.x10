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
        val intermediates = new ArrayList[HashMap[CK, CV]]();
        for (i in input) 
            intermediates.add(mapper.run(i, m_output_collector));

        val shuffled = new HashMap[CK, List[CV]]();
        for (intermediate in intermediates) {
            for (k in intermediate.keySet()) {
                if (shuffled.containsKey(k)) {
                    shuffled.get(k).value.add(intermediate.get(k).value);
                } else {
                    val list = new ArrayList[CV]();
                    list.add(intermediate.get(k).value);
                    shuffled.put(k, list as List[CV]);
                }
            }
        }
        return reducer.run(shuffled, r_output_collector);
    }
}
