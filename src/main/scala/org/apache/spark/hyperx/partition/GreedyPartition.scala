package org.apache.spark.hyperx.partition

import org.apache.spark.SparkContext._
import org.apache.spark.hyperx.VertexId
import org.apache.spark.hyperx.util.HyperUtils
import org.apache.spark.rdd.RDD
import scala.collection.mutable


class GreedyPartition extends PartitionStrategy{
    override private[partition] def search(input: RDD[String]): Unit = {
        hRDD = input.coalesce(k / parallelism, shuffle = true)
                .mapPartitionsWithIndex({(i, p) =>
            val demands = Array.fill(k)(new mutable.HashSet[VertexId]())
            val degrees = Array.fill(k)(0)
            val onepass = p.map{h =>
                val pid = (0 until k).map(i =>
                    (i, demands(i).size * costDemand +
                        degrees(i) * costHyperedge)).minBy(_._2)._1
                HyperUtils.iteratorFromHString(h).foreach(demands(pid).add)
                //degrees(pid) += HyperUtils.countDegreeFromHString(h)
                val pair = HyperUtils.countDetailDegreeFromHString(h)
                degrees(pid) += effectiveCount(pair._1, pair._2).toInt
                (h, pid)
            }
            // todo: a loop
            onepass.map{h =>
                val pair = HyperUtils.countDetailDegreeFromHString(h._1)
                val count = effectiveCount(pair._1, pair._2).toInt
                val extraDemand = (0 until k).map(i =>
                    count - HyperUtils.iteratorFromHString(h._1)
                        .count(demands(i).contains))
                val newPid = ((0 until k).filter(_ != h._2).map(i =>
                    (i, (demands(i).size + extraDemand(i)) * costDemand +
                        (degrees(i) + count) * costHyperedge)).toIterator ++
                    Iterator((h._2, demands(h._2).size * costDemand +
                        degrees(h._2) * costHyperedge)))
                    .minBy(_._2)._1
                if (newPid != h._2) {
                    degrees(h._2) -= count
                    HyperUtils.iteratorFromHString(h._1).foreach(
                        demands(h._2).remove)

                    degrees(newPid) += count
                    HyperUtils.iteratorFromHString(h._1).foreach(
                        demands(newPid).add)
                }
                (h._1, newPid)
            }
        })

        val demands = hRDD.map(h =>
            Tuple2(h._2, HyperUtils.iteratorFromHString(h._1).toSet))
            .reduceByKey(_.union(_)).collect()
        val broadcastDemands = hRDD.context.broadcast(demands)

        vRDD = hRDD.flatMap(h => HyperUtils.iteratorFromHString(h._1))
            .distinct(k / parallelism).mapPartitionsWithIndex{(i, p) =>
            val locals = Array.fill(k)(0)
            p.map{v =>
                val pid = (0 until k).map(i =>
                    (i, (if (broadcastDemands.value(i)._2.contains(v)) 1
                        else 0) * costReplica - locals(i))).maxBy(_._2)._1
                locals(pid) += 1
                (v, pid)
            }.toIterator
        }
    }

    private def effectiveCount(src: Int, dst: Int): Double ={
        src * effectiveSrc + dst * effectiveDst
    }

    private val parallelism = 1
}
