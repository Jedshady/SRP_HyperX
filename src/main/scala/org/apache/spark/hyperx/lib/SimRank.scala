package org.apache.spark.hyperx.lib


import org.apache.spark.Logging
import org.apache.spark.hyperx._
import org.apache.spark.hyperx.util.collection.HyperXOpenHashSet

import scala.reflect.ClassTag
import scala.collection.mutable
/**
  * Created by jedshady on 8/18/16.
  */
object SimRank extends Logging {

//  def run[VD: ClassTag, ED: ClassTag]( hypergraph: Hypergraph[mutable.HashSet[HyperedgeId], mutable.HashSet[VertexId]])
//  :Hypergraph[(mutable.HashSet[HyperedgeId], mutable.HashMap[VertexId, Double]),
//    (mutable.HashSet[VertexId], mutable.HashMap[HyperedgeId, Double])] = {
//    val numV = hypergraph.numVertices
//    val numH = hypergraph.numHyperedges
//    run(hypergraph, 10, hypergraph.pickRandomVertices(numV.toInt),
//      hypergraph.pickRandomHyperEdges(numH.toInt))
//  }
//
//  def run[VD: ClassTag, ED: ClassTag]( hypergraph: Hypergraph[mutable.HashSet[HyperedgeId], mutable.HashSet[VertexId]],
//                                       num: Int, maxIter: Int)
//  :Hypergraph[(mutable.HashSet[HyperedgeId], mutable.HashMap[VertexId, Double]),
//    (mutable.HashSet[VertexId], mutable.HashMap[HyperedgeId, Double])] = {
//    run(hypergraph, maxIter, hypergraph.pickRandomVertices(num),
//      hypergraph.pickRandomHyperEdges(num))
//  }

  def run[VD:ClassTag, ED: ClassTag](hypergraph: Hypergraph[mutable.HashSet[HyperedgeId], mutable.HashSet[VertexId]], numIter: Int,
                                     startVSet: mutable.HashSet[VertexId],
                                     startHSet: mutable.HashSet[HyperedgeId])
  : Hypergraph[(mutable.HashSet[HyperedgeId], mutable.HashMap[VertexId, Double]),
    (mutable.HashSet[VertexId], mutable.HashMap[HyperedgeId, Double])] = {

    val vertexTable = hypergraph.vertices.collect().toMap
    val edgeTable = hypergraph.hyperedges.map(h => (h.id, h.attr)).collect().toMap



    val walkHypergraph
    : Hypergraph[(mutable.HashSet[HyperedgeId], mutable.HashMap[VertexId, Double]),
      (mutable.HashSet[VertexId], mutable.HashMap[HyperedgeId, Double])] =
      hypergraph.mapVertices{(vid, vdata) =>
        val vertexMap = new mutable.HashMap[VertexId, Double]
        startVSet.foreach(id => vertexMap.put(id, 0.0))
        vertexMap.put(vid, 1.0)
        (vdata, vertexMap)
      }.mapHyperedges{ edge =>
        val edgeMap = new mutable.HashMap[HyperedgeId, Double]
        startHSet.foreach(id => edgeMap.put(id, 0.0))
        edgeMap.put(edge.id, 1.0)
        (edge.id, (edge.attr, edgeMap))
      }.cache()

    def vertexProg(id: VertexId, attr: (mutable.HashSet[HyperedgeId], mutable.HashMap[VertexId, Double]),
                   msg: mutable.HashMap[HyperedgeId, mutable.HashMap[HyperedgeId, Double]])
    :(mutable.HashSet[HyperedgeId], mutable.HashMap[VertexId, Double])={

      val updatedAttr = attr

      val currentHSet = attr._1
      val currentHSize = currentHSet.size
      val otherVertex = attr._2 - id

      if(otherVertex.size > 1) {
        otherVertex.foreach { pair =>
          val otherId = pair._1
          val otherHSet = vertexTable.get(otherId).get
          val otherHSize = otherHSet.size

          var sum = 0.0
          currentHSet.foreach { chid =>
            otherHSet.foreach { ohid =>
              val chidAttr = msg.getOrElse(chid, new mutable.HashMap[HyperedgeId, Double]())
              val similarity = chidAttr.getOrElse(ohid, 0.0)
              //TODO: need to add sim(cv, ov) into cv's HashMap
              //            if(similarity == 0){}
              sum = sum + similarity
            }
          }

          val evidenceSize = currentHSet.diff(otherHSet).size
          val evidence = Array[Double](evidenceSize).zipWithIndex
            .map(pair => 1 / math.pow(pair._1, pair._2 + 1))
            .sum

          val finalSimilarity = evidence * sum * 0.8 / currentHSize / otherHSize
          updatedAttr._2.put(otherId, finalSimilarity)
        }
      }
      updatedAttr
    }

    def hyperedgeProg(hyperedge: HyperedgeTuple[(mutable.HashSet[HyperedgeId], mutable.HashMap[VertexId, Double]),
      (mutable.HashSet[VertexId], mutable.HashMap[HyperedgeId, Double])])
    : Iterator[(VertexId, mutable.HashMap[HyperedgeId, mutable.HashMap[HyperedgeId, Double]])] = {
      val currentId = hyperedge.id
      val currentVSet = (hyperedge.srcAttr ++  hyperedge.dstAttr).keySet
      val currentVSize = currentVSet.size
      val otherEdge = hyperedge.attr._2 - currentId

      if (otherEdge.size > 1) {
        otherEdge.iterator.foreach { pair =>
          val otherId = pair._1
          val otherVSet = edgeTable.get(otherId).get
          val otherVSize = otherVSet.size

          var sum = 0.0
          currentVSet.iterator.toArray.foreach { cvid =>
            val cV = new HyperXOpenHashSet[VertexId]
            cV.add(cvid)

            otherVSet.foreach { ovid =>
              val cvidAttr = hyperedge.vertexAttr(cV).toMap.get(cvid).get._2
              val similarity = cvidAttr.getOrElse(ovid, 0.0)
              //TODO: need to add sim(cv, ov) into cv's HashMap
              //            if(similarity == 0){}
              sum = sum + similarity
            }
          }

          val evidenceSize = currentVSet.iterator.toSet.diff(otherVSet.toSet).size
          val evidence = Array[Double](evidenceSize).zipWithIndex
            .map(pair => 1 / math.pow(pair._1, pair._2 + 1))
            .sum

          val finalSimilarity = evidence * sum * 0.8 / currentVSize / otherVSize
          hyperedge.attr._2.put(otherId, finalSimilarity)
        }
      }
      val msgVal = new mutable.HashMap[HyperedgeId, mutable.HashMap[HyperedgeId, Double]]()
      msgVal.put(currentId, hyperedge.attr._2)
      (hyperedge.srcAttr ++ hyperedge.dstAttr).map(attr => (attr._1, msgVal)).toIterator
    }

    def messageCombiner(a: mutable.HashMap[HyperedgeId, mutable.HashMap[HyperedgeId, Double]],
                        b: mutable.HashMap[HyperedgeId, mutable.HashMap[HyperedgeId, Double]])
    : mutable.HashMap[HyperedgeId, mutable.HashMap[HyperedgeId, Double]] = {
      b.foreach{ pair =>
        val id = pair._1
        val value = pair._2
        a.put(id, value)
      }
      a
    }

    val initialMessage = new mutable.HashMap[HyperedgeId, mutable.HashMap[HyperedgeId, Double]]()

    val ret = HyperPregel(walkHypergraph, initialMessage, numIter)(
      vertexProg, hyperedgeProg, messageCombiner)

    ret
  }

}
