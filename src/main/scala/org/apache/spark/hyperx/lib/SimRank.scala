package org.apache.spark.hyperx.lib


import org.apache.spark.Logging
import org.apache.spark.hyperx._

import scala.reflect.ClassTag
import scala.collection.mutable
/**
  * Created by jedshady on 8/18/16.
  */
object SimRank extends Logging {

  def run[VD: ClassTag, ED: ClassTag]( hypergraph: Hypergraph[VD, ED])
  :Hypergraph[(VD, mutable.HashMap[VertexId, Double]), (ED, mutable.HashMap[HyperedgeId, Double])] = {
    val numV = hypergraph.numVertices
    val numH = hypergraph.numHyperedges
    run(hypergraph, 10, hypergraph.pickRandomVertices(numV.toInt),
      hypergraph.pickRandomHyperEdges(numH.toInt))
  }

  def run[VD: ClassTag, ED: ClassTag]( hypergraph: Hypergraph[VD, ED],
                                       num: Int, maxIter: Int)
  :Hypergraph[(VD, mutable.HashMap[VertexId, Double]), (ED, mutable.HashMap[HyperedgeId, Double])] = {
    run(hypergraph, maxIter, hypergraph.pickRandomVertices(num),
      hypergraph.pickRandomHyperEdges(num))
  }

  def run[VD:ClassTag, ED: ClassTag](hypergraph: Hypergraph[VD, ED], numIter: Int,
                                     startVSet: mutable.HashSet[VertexId],
                                     startHSet: mutable.HashSet[HyperedgeId])
  : Hypergraph[(VD, mutable.HashMap[VertexId, Double]), (ED, mutable.HashMap[HyperedgeId, Double])] = {

    val vertexTable = hypergraph.vertices.collect().toMap
    val edgeTable = hypergraph.hyperedges.map(h => (h.id, h.attr)).collect().toMap

    val walkHypergraph
    : Hypergraph[(VD, mutable.HashMap[VertexId, Double]), (ED, mutable.HashMap[HyperedgeId, Double])] =
      hypergraph.mapVertices{(vid, vdata) =>
        val vertexMap = new mutable.HashMap[VertexId, Double]
        vertexMap.put(vid, 1.0)
        startVSet.foreach(id =>vertexMap.put(id, 0.0))
        (vdata, vertexMap)
      }.mapHyperedges{ edge =>
        val edgeMap = new mutable.HashMap[HyperedgeId, Double]
        edgeMap.put(edge.id, 1.0)
        startHSet.foreach(id => edgeMap.put(id, 0.0))
        (edge.id, (edge.attr, edgeMap))
      }.cache()

    def vertexProg(id: VertexId, attr: (VD, mutable.HashMap[VertexId, Double]))

    def hyperedgeProg(hyperedge: HyperedgeTuple[(VD, mutable.HashMap[VertexId, Double]), (ED, mutable.HashMap[HyperedgeId, Double])])
    = {
      val currentId = hyperedge.id
      val otherEdge = hyperedge.attr._2 - currentId
      otherEdge.iterator.foreach{pair=>
        val otherId = pair._1
        val 
      }
    }
  }

}
