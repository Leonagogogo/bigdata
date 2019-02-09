import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.immutable._

object FrequentItem {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    Logger.getLogger("FrequentItem").setLevel(Level.OFF)
    spark.sparkContext.setLogLevel("ERROR")
   // val sc = new SparkContext(new SparkConf().setAppName("FrequentItem"))

    val order = spark.read.option("inferSchema", "true").option("header", "true").csv(args(0)).drop("add_to_cart_order").drop("reordered")
    val product = spark.read.option("inferSchema", "true").option("header", "true").csv(args(1)).drop("aisle_id").drop("department_id")
    val joinData = order.join(product, order.col("product_id") === product.col("product_id"))
    val dataClean = joinData.drop("product_id").rdd

    val data = dataClean.map(x => (x.getAs[Integer](0), x.getAs[String](1)))

    val transactions: RDD[Array[String]] = data.groupByKey().map(x => x._2.toArray)
    transactions.cache()
    val fpg = new FPGrowth().setMinSupport(0.01).setNumPartitions(1)
    val model = fpg.run(transactions)

    val minConfidence = 0.22
    val freqItem = model.freqItemsets.map(x => (x.items.mkString("[", ",", "]"), x.freq)).sortBy(-_._2)
    val freqDF = spark.createDataFrame(freqItem).toDF("item","freq")

    val assoRules = model.generateAssociationRules(minConfidence).map(x => (x.antecedent.mkString("[", ",", "]") + "=>" + x.consequent.mkString("[", ",", "]"), x.confidence)).sortBy(-_._2)
    val assoDF = spark.createDataFrame(assoRules).toDF("antecedent","consequent")


    freqDF.select("item", "freq").write.format("csv").option("header", "true").mode("append").csv(args(2))
    assoDF.select("antecedent", "consequent").write.format("csv").option("header", "true").mode("append").csv(args(2))

    /*model.freqItemsets.take(10).foreach { itemset =>
      println(s"${itemset.items.mkString("[", ",", "]")},${itemset.freq}")
    }

    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(s"${rule.antecedent.mkString("[", ",", "]")}=> " +
        s"${rule.consequent .mkString("[", ",", "]")},${rule.confidence}")
    }*/

  }

}




