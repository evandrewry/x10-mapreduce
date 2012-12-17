X10-MapReduce
=============
COMS W4130 : Principles and Practice of Parallel Programming

Partners : Timothy Giel (tkg2104), Evan Drewry (ewd2106)

Proposal
--------
This project is an attempt to implement a Hadoop style MapReduce engine in X10.

What our Map Reduce engine will allow us to do is to process datasets in parallel.
The goal will be to get our engine to perform as efficiently as possible.

The Map part of the MapReduce engine will perform a user-defined map operation on
the dataset in parallel. Each node will process some subset of the input dataset. 
The processed data will be collected in an OutputCollector object using the
collect(key, value) method, and then passed back to our “master” node, where
additional processing will occur before the data is sent to the reduce job.

Between map and reduce, the master node will "shuffle" the intermediate map output,
which is just sorting the map output by key and creating a HashMap of key, 
list-of-values pairs. The reduce work will be distributed across reducer nodes
according to key, and the reduce jobs will perform some reduction operation on
each list of values (one list per key).

After the reduction work is finished, the output will be returned to the master
node, which will then collect all of the processed data and form the output, which
will be the answer to the original problem.


The Basics - WordCount.x10
--------------------------
Basically, the MapReduce programmer subclasses both Reducer and Mapper with his own
implementations of reduce() and map(), respectively. The best way to see this in 
action is by example. Included in the source, this is the canonical multi-document
word count example of a MapReduce program using our X10 flavor of MapReduce.

###Map
Here is the WordCountMapper subclass of Mapper, which calls collect(word, 1) on
the OutputCollector for each word/token in the input string:
```scala
private static class WordCountMapper extends Mapper[Int, String, String, Int] {
    public def map(num:Int,
                   input:String,
                   output:MapperOutputCollector[String, Int]) {
        var text:String = input;
        var word:String;
        while(text.trim().length() > 0) {
            text = text.trim();
            val end = text.indexOf(' ');
            if (end > 0) {
                word = text.substring(0, end);
                text = text.substring(end);
            } else {
                word = text;
                text = "";
            }
            output.collect(word, 1);
        }
    }
}
```
This code will be executed for each subset of our data set in parallel, and
then the output will be collected and processed by the master node.

###The OutputCollectors
The implementation of collect(key, value) in MapperOutputCollector and 
ReducerOutputCollector basically just adds the pair to a list of intermediate
results. The reason we have two separate data structures for the Map and Reduce
OutputCollectors is that the MapperOutputCollector partitions the intermediate
results using the provided partition function in order to optimize the shuffle
step that occurs between map and reduce.


###Reduce
Once we have collected all the output for each map job, the shuffle step will
occur behind the scenes in the MapReduceJob.x10 code, and this shuffled output
will be sent to the reducer class, which is implemented as follows:
```scala
private static class WordCountReducer extends Reducer[String, Int, String, Int] {
    public def reduce(key:String,
                      values:List[Int],
                      output:ReducerOutputCollector[String, Int]) {
        var sum:Int = 0;
        for (i in 0..(values.size() - 1)) {
            sum += values(i);
        }
        output.collect(key, sum);
    }
}
```

###Putting the pieces together
This is how we would construct and run a MapReduceJob for the word count example,
where input is a list of Pairs that we want to feed into the Mapper class as
input. In this example, each item on the list is a documents we want to count
the words in. The list is split up evenly and each resulting partition is then
sent to a seperate Mapper job, which processes all the documents in its input
list (keys are document id's, values are the actual documents, as Strings).
Each Mapper job will output (key, value) pairs with words as keys, and with counts
as values. The shuffle operation partitions these intermediate results into
groups based on key, and merges all values with matching keys into a single
(key, list-of-values) pair. The shuffle output will again have words as keys,
but rather than a single count as the value, it will have a list of counts from
each Mapper job. Each partition is then sent to a Reduce job individually, where
in this example the sum of all word counts in the list of counts are summed to get
a final word count across all documents.
```scala
val mapper = new WordCountMapper();
val reducer =  new WordCountReducer();
val job = new MapReduceJob(mapper,
                           reducer,
                           (k:String, n:Int) => Math.abs(k.hashCode()) % n);
val output = job.run(input);
```


