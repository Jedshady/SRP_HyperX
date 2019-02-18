Streaming Refinement Partitioning on HyperX
===========

We propose streaming refinement partitioning (SRP) as a variant of the traditional label propagation partitioning (LPP) in streaming scenario. SRP contains two steps.

1. Rough Partitioning: In the first step, we use greedy strategy to partition the incoming vertices. This strategy places each incoming vertex onto the partition where the marginal cost of a pre-defined objective function is minimized. It is efficient as it enjoys a linear time complexity. And we use a heuristic balancing constraint to avoid severe imbalance.

2. Iterative Refinement: In the second step, we put our attention on refining the load-balance. We iteratively refine the partitioning we have obtained from the first step in order to minimize the cost of distributed computing based on our choice of metrics, for example the total number of replicas and the load-balance constraints. We perform a re-partitioning of the hypergraph by running an iterative refinement using label propagation algorithm. We introduce a sliding window on the streaming to limit the number of vertices the partitioning algorithm is running on so that the time and memory constraints in real-time processing is compiled.

More info on HyperX:

HyperX is a scalable framework for hypergraph processing and learning algorithms. HyperX is built upon Apache Spark and inspired by its graph counterpart, GraphX.

When processing a hypergraph (where an edge contains arbitrary number of vertices), instead of converting the hypergraph to a bipartite and employing GraphX to do the tricks, HyperX directly operates on a distributed hypergraph representation. By carefully optimizing the hypergraph partitioning strategies, the preliminary exprimental results show that HyperX is able to achieve a 49 speedup factor on the hypergraph random walks upon the bipartite GraphX solution.

The details of HyperX can be found in following publications:
1. Huang, Jin, Rui Zhang, and Jeffrey Xu Yu. "Scalable hypergraph learning and processing." 2015 IEEE International Conference on Data Mining. IEEE, 2015.
2. Jiang, Wenkai, et al. "HyperX: A Scalable Hypergraph Framework." IEEE Transactions on Knowledge and Data Engineering (2018).

Acknowledgement:
This code was forked from https://github.com/jinhuang/hyperx and modified by Wenkai Jiang afterward.


