/* *****************************************************
* This program requires one argument:
*  file could be downloaded from:
*  https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236
*  we only care about OriginAirportID, OriginCityName, DestAirportID, DestCityName
* *****************************************************/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.graphframes.GraphFrame

object GraphX {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    if (args.length != 2) {
      println("two arguments needed")
    }
    Logger.getLogger("FlightPath").setLevel(Level.OFF)
    spark.sparkContext.setLogLevel("ERROR")

    // in Scala
    val flightStation = spark.read.option("header","true").csv(args(0))

    val flightVertices = flightStation.withColumnRenamed("ORIGIN_CITY_NAME","id").drop("ORIGIN_AIRPORT_ID").drop("DEST_AIRPORT_ID").drop("DEST_CITY_NAME").distinct()

    val flightEdges = flightStation.withColumnRenamed("ORIGIN_CITY_NAME","src").withColumnRenamed("DEST_CITY_NAME","dst").distinct()

    val flightGraph = GraphFrame(flightVertices, flightEdges)

    println(s"Total Number of Airports: ${flightGraph.vertices.count()}")
    println(s"Total Number of Unique Flights Paths: ${flightGraph.edges.count()}")

    val numOfOri = flightGraph.edges.where("src = 'Dallas/Fort Worth, TX'").groupBy("src").count()
    println("Total Number of Unique Flights Paths Origination is Dallas/Fort Worth, TX: ")
    numOfOri.show(false)

    val numOfDst = flightGraph.edges.where("dst = 'Dallas/Fort Worth, TX'").groupBy("dst").count()
    println("Total Number of Unique Flights Paths Destination is Dallas/Fort Worth, TX: ")
    numOfDst.show(false)

    val motifs = flightGraph.find("(a)-[ab]->(b); (b)-[bc]->(c)")
    val filterFlight = motifs.filter("(ab.src = 'Dallas/Fort Worth, TX') and (bc.src = 'Las Vegas, NV') and (bc.dst = 'New York, NY')")
    println("The Path starts from Dallas/Fort Worth, TX to New York, NV, via Las Vegas, NV")
    filterFlight.show(false)

    val inDeg = flightGraph.inDegrees
    println("Airport has the maximum number of starting paths: ")
    inDeg.orderBy(desc("inDegree")).show(1, false)

    val outDeg = flightGraph.outDegrees
    println("Airport has the maximum number of terminating paths: ")
    outDeg.orderBy(desc("outDegree")).show(1, false)

    import org.apache.spark.sql.functions.desc
    val ranks = flightGraph.pageRank.resetProbability(0.15).maxIter(10).run()
    println("The top 10 airports with the highest value of PageRank: ")
    ranks.vertices.orderBy(desc("pagerank")).select("id", "pagerank").show(10)

  }

}
