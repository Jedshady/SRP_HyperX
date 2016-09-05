package org.apache.spark.hyperx.lib

import org.apache.spark.Logging
import org.apache.spark.hyperx._
import org.apache.spark.hyperx.util.collection.HyperXOpenHashMap
import org.apache.spark.rdd.RDD

import scala.collection.mutable
/**
  * Created by jedshady on 9/2/16.
  */
object MaxFlow extends Logging {
  def shortestPath(targetId: VertexId, graph: Hypergraph[Int, HyperXOpenHashMap[VertexId, (Int, Int)]])
  : (mutable.Set[(Long, Int)], Int) ={
    val sourceId = (Int.MaxValue - 2).toLong

    val initialGraph = graph.mapVertices((id,_) => if (id != targetId) (1, 1, (sourceId, 1))
      else (Int.MaxValue - 1, Int.MaxValue - 1, (id, 1))).mapHyperedges{edge =>
      (edge.id, (edge.attr, (Int.MaxValue - 1, Int.MaxValue - 1, (1.toLong, edge.id))))
    }

    val initialgraphV = initialGraph.vertices.collect()

    val RDDVPartitions = initialGraph.vertices.partitions.length
    val RDDHPartitions = initialGraph.hyperedges.partitions.length
    println("============================")
    println("initialgraph")
    println("============================")
    initialgraphV.foreach(v =>
      println("vid = " + v._1 + " vAttr: " + v._2 + " numPart: " + RDDVPartitions + " numPartH: " + RDDHPartitions)
    )

//    def mapFunc(tuple: HyperedgeTuple[(Int, Int, (Long, Int)),
//      (HyperXOpenHashMap[VertexId, (Int, Int)], (Int, Int, (Long, Int)))])
//    :Iterator[(VertexId, Int)] ={
//      val vAttr = tuple.srcAttr ++ tuple.dstAttr
//      vAttr.iterator.foreach(v  =>
//        println("vid: " + v._1 + "vAttr: " + v._2)
//      )
//      vAttr.map(v => (v._1, 1)).iterator
//    }
//
//    val dummyVertices = initialGraph.mapReduceTuples[Int](mapFunc, (a, b) => a + b).collect()

    val initialgraphH = initialGraph.tuples.collect()
    initialgraphH.foreach(t =>
      println("tvattr = " + (t.srcAttr ++ t.dstAttr).toString + " thattr: " + t.attr.toString())
    )
    println("============================")



    def vertexProg(id: VertexId, attr: (Int, Int, (Long, Int)), msg: (Int, Int, (Long, Int)))
    :(Int, Int, (Long, Int)) ={
        if (attr._1 < msg._1) attr
        else msg
    }

    def hyperedgeProg(hyperedge: HyperedgeTuple[(Int, Int, (Long, Int)),
      (HyperXOpenHashMap[VertexId, (Int, Int)], (Int, Int, (Long, Int)))])
    :Iterator[(VertexId, (Int, Int, (Long, Int)))] ={
      
      val edgeAsVertexAttr = hyperedge.attr._2
      val edgeAttr = hyperedge.attr._1
      val vertexAttr = hyperedge.srcAttr ++ hyperedge.dstAttr
      val vertexKeySet = vertexAttr.keySet

      val newVertexAttr = new mutable.HashMap[VertexId, (Int, Int, (Long, Int))]

      vertexKeySet.iterator.foreach{vid =>
        val vAttr = vertexAttr.apply(vid)
        val vIdEdgeAttr = edgeAttr.getOrElse(vid, (0,0))._1
        if(vid != targetId && vAttr._1  + 1 < edgeAsVertexAttr._1 && vIdEdgeAttr > 0){
          newVertexAttr.update(vid, (vAttr._1 + 1, math.min(vAttr._2, vIdEdgeAttr), (vid, 1)))
        }
      }
//      vertexAttr.filter(v => v._1 != targetId).toArray.foreach{ vAttr =>
//        val vId = vAttr._1
//        val vIdEdgeAttr = edgeAttr.getOrElse(vId, (0,0))._1
//        if(vAttr._2._1  + 1 < edgeAsVertexAttr._1 && vIdEdgeAttr > 0){
//          newVertexAttr.update(vId, (vAttr._2._1 + 1, math.min(vAttr._2._2, vIdEdgeAttr), (vId, 1)))
//        }
//      }

      println("new Vertex Attr: " + newVertexAttr.toString())

      val msgToEdge = newVertexAttr.reduce{(a,b) =>
        if (a._2._1 < b._2._1) a
        else if ((a._2._1 == b._2._1) && (a._2._2 > b._2._2)){
          a
        }else b
      }

//      val msgFromEdge = if(edgeAsVertexAttr._1 + 1 < msgToEdge._2._1 ) edgeAsVertexAttr
//      else msgToEdge._2
      val msgFromEdge = msgToEdge._2

      if(vertexAttr.getOrElse(targetId, (0, 0, (targetId, 1)))._1 == Int.MaxValue - 1){
        vertexAttr.filter(v => v._1 == targetId)
          .map(attr => (attr._1, (msgFromEdge._1, msgFromEdge._2, (msgFromEdge._3._1, hyperedge.id)))).toIterator
      }else{
        Iterator.empty
      }
    }

    def messageCombiner(a: (Int, Int, (Long, Int)), b:(Int, Int, (Long, Int)))
    : (Int, Int, (Long, Int))={
      if(a._1 < b._1) a
      else if ((a._1 == b._1) && (a._2 > b._2)){
        a
      }else b
//      b
    }

    val initialMessage = (Int.MaxValue - 1, Int.MaxValue - 1, (targetId, 1))

    val ret = HyperPregel(initialGraph, initialMessage)(
      vertexProg, hyperedgeProg, messageCombiner)


    val pathSet = mutable.Set[(Long, Int)]()
    val path = ret.vertices.filter(v=> v._1 == targetId).collect()
    if(path.head._2._3._1 != targetId) {
       val pathValue = path.head._2._3
       val pathStart = pathValue._1
       val pathTail = pathValue._2
       pathSet += ((pathStart, pathTail))
    }

    println("path: " + pathSet.toString())
    println("==========================")

    (pathSet, 1)
  }


  def run(targetId: VertexId, graph:Hypergraph[Int, Int])
  : RDD[((VertexId, HyperedgeId), Int)] ={

    val walkgraph: Hypergraph[Int, HyperXOpenHashMap[VertexId, (Int, Int)]] = graph.mapHyperedges{ edge =>
      val hId = edge.id
      val vSet = edge.srcIds.iterator ++ edge.dstIds.iterator
      val map = new HyperXOpenHashMap[VertexId, (Int, Int)]
      vSet.foreach{vId =>
        if (vId != targetId){
          map.update(vId, (1, 0))
        }
      }
      (hId, map)
    }

    var flows: RDD[((VertexId, HyperedgeId), Int)] = graph.hyperedges.flatMap{edge =>
      val hId = edge.id
      val vSet = edge.srcIds.iterator ++ edge.dstIds.iterator
      vSet.map(vId => ((vId, hId),0))
    }

    var residual: Hypergraph[Int, HyperXOpenHashMap[VertexId, (Int, Int)]] = walkgraph

    val walkGraphV = walkgraph.vertices.collect()
    val walkGraphH = walkgraph.hyperedges.collect()

    println("============================")
    walkGraphV.foreach(v =>
      println("vid = " + v._1 + " vAttr: " + v._2)
    )
    walkGraphH.foreach(h =>
      println("hid = " + h.toString)
    )
    println("============================")

    var shortest = shortestPath(targetId, residual)
    var path = shortest._1
    var minCap = shortest._2

    while (path.nonEmpty){
      val bcPath = flows.sparkContext.broadcast(path)
      val updateFlow = minCap

      val newFlows = flows.map{ e =>
        if (bcPath.value.apply(e._1)){
          (e._1, e._2 + updateFlow)
        }else {
          e
        }
      }

      flows = newFlows

      residual = residual.mapHyperedges{e =>
        val id =  e.id
        val pathId = bcPath.value.last._2
        val pathVId = bcPath.value.last._1
        if(id == pathId){
          e.attr.update(pathVId, (0, 1))
          (id, e.attr)
        }else{
          (id, e.attr)
        }
      }

      shortest = shortestPath(targetId, residual)
      path = shortest._1
      minCap = shortest._2
    }
    val flowsFinal = flows.collect().toSet
    flowsFinal.foreach(v =>
      println("flowsFinal: " + v.toString())
    )
    flows
  }
}
