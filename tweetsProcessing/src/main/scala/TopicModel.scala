import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.sql.types.FloatType


object TopicModel {

  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder.appName("TopicModel").getOrCreate()
    Logger.getLogger("TopicModel").setLevel(Level.OFF)
    spark.sparkContext.setLogLevel("ERROR")

    val tweets = spark.read.option("inferSchema","true").option("header","true").csv(args(0)).filter("text is not null")
    val tweetsReplace1 = tweets.na.replace("airline_sentiment", Map("neutral" -> "2.5"))
    val tweetsReplace2 = tweetsReplace1.na.replace("airline_sentiment", Map("positive" -> "5.0"))
    val tweetsReplace = tweetsReplace2.na.replace("airline_sentiment", Map("negative" -> "1.0"))
    val tweetsData = tweetsReplace.withColumn("airline_sentiment", tweetsReplace.col("airline_sentiment").cast(sql.types.FloatType))
    val cleanData = tweetsData.select("airline","airline_sentiment","text").rdd
    val mappedData = cleanData.map(x => (x.getString(0), x.getFloat(1), x.getString(2)))
    val mappedTweets = spark.createDataFrame(mappedData).toDF("airline","airline_sentiment","text")
    mappedTweets.cache()
    val avgStats = mappedTweets.groupBy("airline").avg("airline_sentiment")
    val bestAir = avgStats.orderBy(desc("avg(airline_sentiment)")).first()
    val worstAir = avgStats.orderBy(asc("avg(airline_sentiment)")).first()
    val bestAirprint = "Best airline is " + bestAir.getString(0) + " with average rating of " + bestAir.getDouble(1)
    val worstAirprint = "Worst airline is " + worstAir.getString(0) + " with average rating of " + worstAir.getDouble(1)
    println(s"Best airline is %s with average rating of %f".format(bestAir.getString(0), bestAir.getDouble(1)))
    println(s"Worst airline is %s with average rating of %f".format(worstAir.getString(0), worstAir.getDouble(1)))
    val bestAirData = mappedTweets.filter(mappedTweets.col("airline") === bestAir.getString(0)).rdd
    val worstAirData = mappedTweets.filter(mappedTweets("airline") === worstAir.getString(0)).rdd

    val bestAircorpus: RDD[String] = bestAirData.map(x=>x.getString(2)).flatMap(x=>x.split(","))
    val worstAircorpus: RDD[String] = worstAirData.map(x=>x.getString(2)).flatMap(x=>x.split(","))
    val bestAirtokenized: RDD[Seq[String]] = bestAircorpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))
    val worstAirtokenized: RDD[Seq[String]] = worstAircorpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(_.forall(java.lang.Character.isLetter)))
    val bestAirtermCounts: Array[(String, Long)] = bestAirtokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
    val worstAirtermCounts: Array[(String, Long)] = worstAirtokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
    val numStopwords = 20
    val bestAirvocabArray: Array[String] = bestAirtermCounts.takeRight(bestAirtermCounts.size - numStopwords).map(_._1)
    val worstAirvocabArray: Array[String] = worstAirtermCounts.takeRight(worstAirtermCounts.size - numStopwords).map(_._1)
    val bestAirvocab: Map[String, Int] = bestAirvocabArray.zipWithIndex.toMap
    val worstAirvocab: Map[String, Int] = worstAirvocabArray.zipWithIndex.toMap

    val bestAirdocuments: RDD[(Long, Vector)] =
      bestAirtokenized.zipWithIndex.map { case (tokens, id) =>
        val counts = new mutable.HashMap[Int, Double]()
        tokens.foreach { term =>
          if (bestAirvocab.contains(term)) {
            val idx = bestAirvocab(term)
            counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
          }
        }
        (id, Vectors.sparse(bestAirvocab.size, counts.toSeq))
      }

    val worstAirdocuments: RDD[(Long, Vector)] =
      worstAirtokenized.zipWithIndex.map { case (tokens, id) =>
        val counts = new mutable.HashMap[Int, Double]()
        tokens.foreach { term =>
          if (worstAirvocab.contains(term)) {
            val idx = worstAirvocab(term)
            counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
          }
        }
        (id, Vectors.sparse(worstAirvocab.size, counts.toSeq))
      }

    val numTopics = 3
    val lda = new LDA().setK(numTopics).setMaxIterations(10)

    val bestAirldaModel = lda.run(bestAirdocuments)
    val worstAirldaModel = lda.run(worstAirdocuments)

    val bestAirtopicIndices = bestAirldaModel.describeTopics(10).map({case (terms, termWeights) => terms.zip(termWeights).map({case (term, weight) => (bestAirvocabArray(term.toInt), weight)})}).flatMap(x=>x)
    val worstAirtopicIndices = worstAirldaModel.describeTopics(10).map({case (terms, termWeights) => terms.zip(termWeights).map({case (term, weight) => (worstAirvocabArray(term.toInt), weight)})}).flatMap(x=>x)

    val best = spark.createDataFrame(bestAirtopicIndices).toDF("Bwords","Bvalues")
    val worst = spark.createDataFrame(worstAirtopicIndices).toDF("Wwords","Wvalues")

    best.select("Bwords","Bvalues").write.format("csv").option("header", "true").mode("append").csv(args(1))
    worst.select("Wwords","Wvalues").write.format("csv").option("header", "true").mode("append").csv(args(1))

   /* val bestAirtopic = bestAirldaModel.describeTopics(maxTermsPerTopic = 10)
    val worstAirtopic = worstAirldaModel.describeTopics(maxTermsPerTopic = 10)
    bestAirtopic.foreach { case (terms, termWeights) =>
      println("BestAir Topic:")
      terms.zip(termWeights).foreach { case (term, weight) =>
        println(s"${bestAirvocabArray(term.toInt)}\t$weight")
      }
      println()
    }

    worstAirtopic.foreach { case (terms, termWeights) =>
      println("WorstAir Topic:")
      terms.zip(termWeights).foreach { case (term, weight) =>
        println(s"${worstAirvocabArray(term.toInt)}\t$weight")
      }
      println()
    }*/

  }

}
