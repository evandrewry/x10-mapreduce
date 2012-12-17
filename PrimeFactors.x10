import x10.io.Console;
import x10.util.HashMap;
import x10.util.List;
import x10.util.ArrayList;
import x10.util.Pair;
import x10.util.Timer;


/**
* this is a simple test case for our mapreduce framework that
* computes prime factors of the input set, categorizes each
* input by its largest prime factor, and then returns a count
* for each largest prime factor*/

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


    /* MAPPER SUBCLASS */
    private static class PrimeFactorsMapper extends Mapper[Long, Long, Long, Long] {
        public def map(num:Long,
            input:Long,
            output:MapperOutputCollector[Long, Long]) {
            val f = primeFactors(num);
            if (!f.isEmpty()) output.collect(f.getLast(), num);
        }
    }


    /* REDUCER SUBCLASS */    
    private static class PrimeFactorsReducer extends Reducer[Long, Long, Long, Int] {
        public def reduce(key:Long,
            values:List[Long],
            output:ReducerOutputCollector[Long, Int]) {
            /* this is bogus but adds computation to the reduce step
            * and helps in testing speedups */
            primeFactors(9999999999999L);
            output.collect(key, values.size());
        }
    }


    public static def main(argv:Array[String]{self.rank == 1}) {
        if (argv.size != 1) {
            Console.ERR.println("USAGE: PrimeFactors <inputSize>");
            return;
        }
        val set_size = Long.parse(argv(0));


        /* set up input set */
        val input = new ArrayList[Pair[Long, Long]]();
        for (var i:Long = 1; i < set_size; i++) {
            input.add(Pair[Long, Long](i, Long.MAX_VALUE - i));
        }


        /* set up job */
        val mapper = new PrimeFactorsMapper();
        val reducer =  new PrimeFactorsReducer();
        val job = new MapReduceJob[Long, Long, Long, Long, Long, Int](mapper,
            reducer,
            (k:Long, n:Int) => Math.abs(k.hashCode()) % n);


        /* run job */
        val start = timer.milliTime();
        val output = job.run(input);
        val time = timer.milliTime() - start;
        Console.OUT.println("\t\t" + time);

        /* print results */        
        //for (k in output.keySet())
        //Console.OUT.print("[" + k + " : " + output.get(k).value + "], ");
    }
}

