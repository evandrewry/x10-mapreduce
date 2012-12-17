import x10.io.Console;
import x10.util.HashMap;
import x10.util.List;
import x10.util.ArrayList;
import x10.util.Pair;
import x10.util.Timer;

public class PrimeFactors {
    private static val timer = new Timer();

    public static def primeFactors(num:Long) 
    {
        var n:Long = num; 
        var factors:ArrayList[Long] = new ArrayList[Long]();

        for (var i:Long = 2; i <= n / i; i++)
            {
            while (n % i == Zero.get[Long]()) 
                {
                factors.add(i);
                n /= i;
            }
        }
        if (n > 1)
            {
            factors.add(n);
        }
        return factors;
    }

    private static class PrimeFactorsMapper extends Mapper[Long, Long, Long, Long] {
        public def map(num:Long,
            input:Long,
            output:MapperOutputCollector[Long, Long]) {
            val f = primeFactors(num);
            if (!f.isEmpty()) output.collect(f.getLast(), num);
        }
    }

    private static class PrimeFactorsReducer extends Reducer[Long, Long, Long, Int] {
        public def reduce(key:Long,
            values:List[Long],
            output:ReducerOutputCollector[Long, Int]) {
            output.collect(key, values.size());
        }
    }


    public static def main(argv:Array[String]{self.rank == 1}) {
        if (argv.size != 1) {
            Console.ERR.println("USAGE: PrimeFactors <inputSize>");
            return;
        }
        val set_size = Long.parse(argv(0));

        val input_set = new ArrayList[Pair[Long, Long]]();
        for (var i:Long = 1; i < set_size; i++) {
            input_set.add(Pair[Long, Long](i, Long.MAX_VALUE - i));
        }

        val input = new Rail[ArrayList[Pair[Long, Long]]](16, (i:Int) => input_set);

        val mapper = new PrimeFactorsMapper();
        val reducer =  new PrimeFactorsReducer();
        val job = new MapReduceJob[Long, Long, Long, Long, Long, Int](mapper,
            reducer,
            (k:Long, n:Int) => Math.abs(k.hashCode()) % n);

        val start = timer.milliTime();
        val output = job.run(input);
        val time = timer.milliTime() - start;
        Console.OUT.println("\t\t" + time);

        //for (k in output.keySet())
        //Console.OUT.print("[" + k + " : " + output.get(k).value + "], ");
    }
}

