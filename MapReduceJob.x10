import x10.util.HashMap;
import x10.util.List;
import x10.util.ArrayList;

public class MapReduceJob[IK, IV, CK, CV, OK, OV] {
    private val mapper:Mapper[IK, IV, CK, CV];
    private val reducer:Reducer[CK, CV, OK, OV];

    public def this(mapper:Mapper[IK, IV, CK, CV],
                    reducer:Reducer[CK, CV, OK, OV]) {
        this.mapper = mapper;
        this.reducer = reducer;
    }

    public def run(input:HashMap[IK, IV])
    :HashMap[OK, OV] {
        val intermediate = mapper.run(input, new OutputCollector[CK, CV]());
        val shuffled = new HashMap[CK, List[CV]]();
        for (k in intermediate.keySet()) {
            if (shuffled.containsKey(k)) {
                shuffled.get(k).value.add(intermediate.get(k).value);
            } else {
                val list = new ArrayList[CV]();
                list.add(intermediate.get(k).value);
                shuffled.put(k, list as List[CV]);
            }
        }
        return reducer.run(shuffled, new OutputCollector[OK, OV]());
    }
}
