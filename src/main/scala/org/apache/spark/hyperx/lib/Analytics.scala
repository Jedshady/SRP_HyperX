package org.apache.spark.hyperx.lib

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.hyperx.partition._
import org.apache.spark.hyperx.{Hypergraph, HypergraphLoader}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Logging, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * The driver program for all the hypergraph algorithms
 */
object Analytics extends Logging {
    def main(args: Array[String]) = {
        if (args.length < 2) {
            System.err.println(
                "Usage: Analytics <taskType> <file> " +
                        "--numPart=<num_partitions> " +
                        "--inputMode=<list|object|plist> " +
                        "--additionalInput=<vertex object file path> " +
                        "--separator=<character> " +
                        "--vertexLevel=<MEMORY_ONLY|MEMORY_ONLY_SER> " +
                        "--hyperedgeLevel=<MEMORY_ONLY|MEMORY_ONLY_SER> " +
                        "--weighted=<true|false> " +
                        "--partStrategy=<Plain|Greedy>" +
                        "--outputPath=<output path> " +
                        "--objectiveH=<double> " +
                        "--objectiveV=<double> " +
                        "--objectiveNorm=<integer> " +
                        "--effectiveSrc=<double> " +
                        "--effectiveDst=<double> " +
                        "[other options]")
            System.exit(1)
        }

        val taskType = args(0)
        val fname = args(1)
        val optionsList = args.drop(2).map { arg =>
            arg.dropWhile(_ == '-').split('=') match {
                case Array(opt, v) => opt -> v
                case _ => throw new IllegalArgumentException("Invalid " +
                        "argument: " + arg)
            }
        }
        val options = mutable.Map(optionsList: _*)

        def pickPartitioner(v: String): PartitionStrategy = {
            v match {
                case "Plain" => new PlainPartition
                case "Greedy" => new GreedyPartition
                case "Bi" => new BipartitePartition
                case "Aweto" => new AwetoPartition
                case "Replica" => new ReplicaPartition
                case _ => throw new IllegalArgumentException("Invalid " +
                        "PartitionStrategy: " + v)
            }
        }

        val conf = new SparkConf()
                .set("spark.serializer", "org.apache.spark.serializer" +
                ".KryoSerializer")
                .set("spark.kryo.registrator", "org.apache.spark.hyperx" +
                ".HypergraphKryoRegistrator")
                .set("spark.locality.wait", "100000")

        val numPart = options.remove("numPart").map(_.toInt).getOrElse {
            println("Set the number of partitions using --numPart.")
            sys.exit(1)
        }

        val inputMode = options.remove("inputMode").getOrElse(listMode)
        val vertexInput = options.remove("additionalInput").getOrElse{
            if (inputMode.equals(objectMode)) {
                println("Set the vertex input using " +
                        "--additionalInput when inputMode is object file")
                sys.exit(1)
            }
            else ""
        }

        val outputPath = options.remove("outputPath").getOrElse(test_path)

        val partitionStrategy = options.remove("partStrategy")
                .map(pickPartitioner).getOrElse(new PlainPartition)
        val hyperedgeStorageLevel = options.remove("hyperedgeLevel")
                .map(StorageLevel.fromString).getOrElse(StorageLevel
                .MEMORY_ONLY)
        val vertexStorageLevel = options.remove("vertexLevel")
                .map(StorageLevel.fromString).getOrElse(StorageLevel
                .MEMORY_ONLY)
        val fieldSeparator = options.remove("separator").getOrElse(":")
        val weighted = options.remove("weighted").exists(_ == "true")

        val objectiveH = options.remove("objectiveH").map(_.toDouble).getOrElse(0.5)
        val objectiveV = options.remove("objectiveV").map(_.toDouble).getOrElse(1.0)
        val objectiveD = options.remove("objectiveD").map(_.toDouble).getOrElse(0.8)
        val objectiveNorm = options.remove("objectiveNorm").map(_.toInt).getOrElse(2)
        val effectiveSrc = options.remove("effectiveSrc").map(_.toDouble).getOrElse(1.0)
        val effectiveDst = options.remove("effectiveDst").map(_.toDouble).getOrElse(1.0)

        val URL = options.remove("url").getOrElse(test_url)
        val statisticOut = options.remove("staOut").getOrElse(test_StaOut)
        val expNumber = options.remove("expNum").getOrElse("0")

        partitionStrategy.setPartitionParams(objectiveH, objectiveV, objectiveD,
            objectiveNorm, effectiveSrc, effectiveDst)

        taskType match {
            case "load" => // loading test
                println("==========================")
                println("| Loading Test |")
                println("==========================")

                val sc = new SparkContext(conf.setAppName("Loading Test (" +
                        fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel)

                hypergraph.cache()

                println("==========================")
                println("HYPERX: Number of vertices: " +
                        hypergraph.vertices.count)
                println("HYPERX: Number of hyperedges: " +
                        hypergraph.hyperedges.count)
                println("==========================")

                sc.stop()

            case "test" =>
                val sc = new SparkContext(conf.setAppName("Testing (" +
                        fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel)
                        .cache()

                val testHypergraph = hypergraph.mapTuples(h => (h.id, 0)).cache()

                val sum = testHypergraph.hyperedges.map(h => h.attr).reduce(_ + _)

                logInfo("Result " + sum)

            case "part" =>

                val sc = new SparkContext(conf.setAppName("Partitioning (" +
                        fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel).cache()

                val name = inputName(fname)

                val edge = hypergraph.hyperedges.partitionsRDD.flatMap[String]{part =>
                    part._2.tupleIterator(true, true).map{tuple =>
                        tuple.attr + " : " + tuple.srcAttr.map(_._1.toString()).reduce(_ + " " + _) + " : " + tuple.dstAttr.map(_._1.toString()).reduce(_+ " " + _)
                    }
                }

                val parStrat = options.remove("partStrategy").getOrElse("parStrat")
                writeResultToHDFS(hypergraph, parStrat, name, URL, statisticOut, expNumber)

                //output partition result to HDFS
                edge.saveAsTextFile(outputPath + parStrat + "/" + name + "/experiment_" + expNumber)

//                hypergraph.hyperedges.saveAsObjectFile(outputPath + name + "/hyperedges")
//                hypergraph.vertices.saveAsObjectFile(outputPath + name + "/vertices")
                sc.stop()

            case "rw" =>

                conf.set("hyperx.debug.k", numPart.toString)
                val sc = new SparkContext(conf.setAppName("Random Walks (" +
                        fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel)
                    .cache()

                val maxIter = options.remove("maxIter").map(_.toInt).getOrElse(10)
                val num= options.remove("numStartVertices").map(_.toInt)
                        .getOrElse(hypergraph.numVertices.toInt / 1000)

                val startSet = hypergraph.pickRandomVertices(num)
                val ret = RandomWalk.run(hypergraph, maxIter, startSet)
                ret.vertices.saveAsTextFile(outputPath + "rw")
                sc.stop()

            case "lp" =>

                conf.set("hyperx.debug.k", numPart.toString)
                val sc = new SparkContext(conf.setAppName("Label Propagation (" +
                        fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel)
                    .cache()

                val maxIter = options.remove("maxIter").map(_.toInt).getOrElse(100)
                val ret = LabelPropagation.run(hypergraph, maxIter)

                ret.vertices.saveAsTextFile(outputPath + "lp")

                sc.stop()

            case "lpp" =>

                conf.set("hyperx.debug.k", numPart.toString)
                val sc = new SparkContext(conf.setAppName("Label Propagation (" +
                    fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel)
                    .cache()

                val maxIter = options.remove("maxIter").map(_.toInt).getOrElse(100)
                val ret = LabelPropagationPartition.run(hypergraph, maxIter, numPart)

                val edge = ret._1

                //TODO: compute statistic value
                val graph = ret._2
                val name = inputName(fname)

                writeResultToHDFS(graph, "lpp", name, URL, statisticOut, expNumber)

                //output partition result to HDFS
                edge.saveAsTextFile(outputPath + "lpp/" + name + "/experiment_" + expNumber)

//                println("==============================")
//                edgePart.foreach(edge =>
//                    println("partitionId: " + edge._1 + " set: " + edge._2.toString())
//                )
//                edgePartReduce.foreach(edge =>
//                    println("partitionId: " + edge._1 + " set: " + edge._2.toString())
//                )
//                vertexPart.foreach(v =>
//                    println("partitionId: " + v._1 + " vID: " + v._2)
//                )
//                vertexPartReduce.foreach(v =>
//                      println("partitionId: " + v._1 + " vID: " + v._2)
//                )
//                println("==============================")

                sc.stop()

            case "bc" =>

                val sc = new SparkContext(conf.setAppName("Between Centrality (" +
                        fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel)
                        .cache()

                val ret = BetweennessCentrality.run(hypergraph)
                ret.filter(v => v._2 > 0.0).saveAsTextFile(outputPath + "bc")
                sc.stop()

            case "statistics" =>
                val sc = new SparkContext(conf.setAppName("Statistics (" + fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel).cache()

                val incidents = hypergraph.incidents
                incidents.saveAsTextFile(outputPath + "statistics")

                sc.stop()

            case "laplacian" =>
                val sc = new SparkContext(conf.setAppName("Laplacian (" + fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel).cache()

                val laplacian = hypergraph.laplacian
                laplacian.saveAsTextFile(outputPath + "laplacian")

                sc.stop()
            case "spectral" =>
                val sc = new SparkContext(conf.setAppName("Spectral (" + fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel).cache()
                val maxIter = options.remove("maxIter").map(_.toInt).getOrElse(10)
                val ret = SpectralLearning.run(hypergraph, 3, maxIter, 1e-9)
                logInfo("HYPERX DEBUGGING: spectral finished")
                sc.parallelize(ret._2).saveAsTextFile(outputPath + "spectral")

                sc.stop()

            case "smrt" =>
                val sc = new SparkContext(conf.setAppName("Spectral (" + fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel).cache()
                val maxIter = options.remove("maxIter").map(_.toInt).getOrElse(10)
                val ret = SpectralLearning.runMRT(hypergraph, 3, maxIter, 1e-9)
                logInfo("HYPERX DEBUGGING: spectral finished")
                sc.parallelize(ret._2).saveAsTextFile(outputPath + "spectral")

                sc.stop()

            case "matrix" =>
                val sc = new SparkContext(conf.setAppName("Spectral (" + fname + ")"))

                val hypergraph = loadHypergraph(sc, fname, vertexInput,
                    fieldSeparator, weighted, numPart, inputMode,
                    partitionStrategy, hyperedgeStorageLevel, vertexStorageLevel).cache()
                println("HYPERX DEBUGGING " + hypergraph.vertices.map(_._1.toString).reduce(_ + " " + _))
                val maxIter = options.remove("maxIter").map(_.toInt).getOrElse(10)
                SpectralLearning.runTest(hypergraph)
                logInfo("HYPERX DEBUGGING: spectral finished")

                sc.stop()
        }
    }

    private def loadHypergraph(sc: SparkContext, fname: String, vfname: String,
        fieldSeparator: String, weighted: Boolean, numPart: Int,
        inputMode: String, partitionStrategy: PartitionStrategy,
        hyperedgeLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
        vertexLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
    : Hypergraph[Int, Int] = {
        val hypergraph = inputMode match {
            case `listMode` =>
                HypergraphLoader.hyperedgeListFile(sc, fname,
                    fieldSeparator, weighted, numPart, partitionStrategy,
                    hyperedgeLevel, vertexLevel)
            case `objectMode` =>
                HypergraphLoader.hypergraphObjectFile(sc, vfname, fname, numPart,
                    hyperedgeLevel, vertexLevel)
            case `partitionedListMode` =>
                HypergraphLoader.partitionFile(sc, fname, numPart, fieldSeparator,
                    hyperedgeLevel, vertexLevel)
        }
        hypergraph
    }

    private def inputName(path: String): String = {
        path.split("/").last
    }

    private def writeResultToHDFS(graph: Hypergraph[Int, Int], parStrat: String,
                                  name: String, URL: String, statisticOut: String, expNumber: String): Unit ={

        //TODO: compute statistic value

        var numOfReplica = Array[Int]()
        var numVertex = Array[Int]()
        var numVertexInEdge = Array[Int]()

        val edgePart = graph.hyperedges.collect().map(edge => (edge.attr, (edge.srcIds.iterator ++ edge.dstIds.iterator).toSet))
        val vertexPart = graph.vertices.collect().map(v => (v._2, v._1))

        val edgePartReduce = edgePart.groupBy(h => h._1).map{item =>
            (item._1, item._2.flatMap(tuple => tuple._2).toSet)
        }

        val vertexPartReduce = vertexPart.groupBy(v => v._1).map(item => (item._1, item._2.map(tuple => tuple._2).toSet))

        edgePartReduce.foreach { h =>
            val pId = h._1
            val demands = h._2
            val local = vertexPartReduce.get(pId).get
            numOfReplica = numOfReplica :+ demands.diff(local).size
            numVertex = numVertex :+ local.size
            numVertexInEdge = numVertexInEdge :+ demands.size

            //                  println("demands: " + demands.toString())
            //                  println("local: " + local.toString())
            //                  println("replica: " + demands.diff(local).toString())
        }

        val replicaFactor = numOfReplica.sum.toDouble / numVertex.sum.toDouble

        val meanEdge = numVertexInEdge.sum.toDouble / numVertexInEdge.length.toDouble
        val devsEdge = numVertexInEdge.map(score => Math.pow(score - meanEdge, 2))
        val stddevEdge = Math.sqrt(devsEdge.sum / numVertexInEdge.length)
        val coefficientOfVariationEdge = stddevEdge / meanEdge

        val meanVertex = numVertex.sum.toDouble / numVertex.length.toDouble
        val devsVertex = numVertex.map(score => Math.pow(score - meanVertex, 2))
        val stddevVertex = Math.sqrt(devsVertex.sum / numVertex.length)
        val coefficientOfVariationVertex = stddevVertex / meanVertex


        //                println("url: " + URL)
        //                println("statisticOut: " + statisticOut)
        //                println("expNumber: " + expNumber)

        // output statistic to HDFS

        val hadoopConf = new Configuration()
        hadoopConf.set("fs.defaultFS", URL)
        val hadoopFS = FileSystem.get(hadoopConf)
        val hadoopOS = hadoopFS.create(new Path(statisticOut + parStrat + "/" + name + "/statistic/" + expNumber))
        hadoopOS.write(("replicaFactor: " + replicaFactor + "\n" +
          "edgeBalance: " + coefficientOfVariationEdge + "\n" +
          "vertexBalance: " + coefficientOfVariationVertex).getBytes)

        //output partition result to HDFS
//        edge.saveAsTextFile(outputPath + "lpp/" + name + "/experiment_" + expNumber)
    }


    private val test_path = "hdfs://master:9000/apps/hyperx/output/"
    private val test_url = "hdfs://wjnamenode:9000"
    private val test_StaOut = "/user/ec2-user/apps/hyperx/output/"

    private val listMode = "list"
    private val objectMode = "object"
    private val partitionedListMode = "plist"
}
