import x10.util.HashMap;
import x10.util.List;
import x10.util.Pair;
import x10.util.Timer;
import x10.util.ArrayList;

/**
 * This class collects all the pieces of MapReduce into one "job". Because the
 * Mappers, reducers, jobs, and actual data input are all modular and separated
 * from one another, they can be reused and mixed/matched as long as they are
 * of compatible types.
 *
 * The actual concurrent logic that makes our framework scale to many threads
 * is all here in the run() function.
 *
 */
public class MapReduceJob[IK, IV, CK, CV, OK, OV] {
    private static val timer = new Timer();
    private val mapper:Mapper[IK, IV, CK, CV];
    private val reducer:Reducer[CK, CV, OK, OV];
    private val partition_op:(CK, Int) => Int;

    /* contruct a new job with specifed mapper, reducer, and partition functions. */
    public def this(mapper:Mapper[IK, IV, CK, CV],
                    reducer:Reducer[CK, CV, OK, OV],
                    partition_op:(CK, Int)=>Int) {
        this.mapper = mapper;
        this.reducer = reducer;
        this.partition_op = partition_op;
    }


    /* the bulk of the MapReduce logic: map, shuffle, reduce, collect */
    public def run(input:List[Pair[IK,IV]])
    {
        val nthreads = Runtime.NTHREADS;
        var start:Long = timer.milliTime();




        /* STEP 1: MAP */
        val intermediates = new Rail[Rail[ArrayList[Pair[CK, CV]]]](nthreads);
        val step = (input.size() < nthreads) ? 1 : (input.size() / nthreads);
        /* divide up input, send to workers */
        finish for (var lower:Int = 0, i:Int = 0; lower < input.size(); lower += step, i++){
              val ii = i;
              val l = lower;
              val u = (lower + step >= input.size()) ? input.size() : (lower + step);
              async {
                  intermediates(ii) = mapper.run(input.subList(l, u), nthreads, partition_op);
              } 
        }
        Console.OUT.println("map\t\t" + (timer.milliTime() - start));




        /* STEP 2: SHUFFLE INTERMEDIATE RESULTS */
        start = timer.milliTime();
        val shuffled = new Rail[HashMap[CK, List[CV]]](nthreads, (i:Int)=>new HashMap[CK, List[CV]]());
        /* iterate over intermediate results */
        for (mapresult in intermediates) {
            /* iterate over partitions for each intermediates result */
            for (partition in intermediates(mapresult)) {
                /* iterate over each key, value pair in partition */
                for (pair in intermediates(mapresult)(partition)) {
                    val k = pair.first;
                    val v = pair.second;
                    val p = shuffled(partition);
                    if (p.containsKey(k)) {
                        p.get(k).value.add(v);
                    } else {
                        val list = new ArrayList[CV]();
                        list.add(v);
                        p.put(k, list as List[CV]);
                    }
                }
            }
        }
        Console.OUT.println("shuffle\t\t" + (timer.milliTime() - start));




        /* STEP 3: REDUCE */
        start = timer.milliTime();
        val reduced = new Rail[ArrayList[Pair[OK, OV]]](nthreads);
        finish for (i in shuffled) async {
            reduced(i) = reducer.run(shuffled(i));
        }
        Console.OUT.println("reduce\t\t" + (timer.milliTime() - start));


        /* STEP 4: COLLECT */
        val output = new HashMap[OK, OV]();
        start = timer.milliTime();
        for (i in reduced) {
            for (pair in reduced(i)) {
                output.put(pair.first, pair.second);
            }
        }
        Console.OUT.println("collect\t\t" + (timer.milliTime() - start));

        /* FINAL RESULT */
        return output;
    }
}
