X10-MapReduce
=============
COMS W4130 : Principles and Practice of Parallel Programming

Partners : Timothy Giel (tkg2104), Evan Drewry (ewd2106)

Proposal
--------
The project that we want to do is to implement a Hadoop style MapReduce engine
in X10.

What our Map Reduce engine will allow us to do is to process datasets in
parallel.  Our goal will be to get our engine to perform as efficiently as
possible.

The Map part of the Map Reduce engine will be where we divide our input into
smaller subsets.  If necessary, these can be divided again.  From here, these
subsets of data will be processed and then passed back to our “master” node.

The master node will then "shuffle" the map output and send the shuffled
intermediate data to the reducer job.

The Reduce part will then collect all of the data that is processed in the
subsets and form the output which will be the answer to the original problem.
