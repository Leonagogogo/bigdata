import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.Pipeline


object Pipeline {
  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    Logger.getLogger("Pipleline").setLevel(Level.OFF)
    spark.sparkContext.setLogLevel("ERROR")

    // pre-processing data
    val rawTweets = spark.read.option("header","true").option("inferSchema","true").csv(args(0))
    val noNullTweets = rawTweets.filter("text is not null").select("airline","airline_sentiment", "text")

    def remove_string1: String => String = _.replaceAll("\"", "")
    def remove_string_udf1 = udf(remove_string1)

    def remove_string3: String => String = _.replaceAll("@", "")
    def remove_string_udf3 = udf(remove_string3)

    def remove_string4: String => String = _.replaceAll("""[\p{Punct}]""", "")
    def remove_string_udf4 = udf(remove_string4)


    val cleanTweets = noNullTweets
      .withColumn("text", remove_string_udf1(col("text")))
      .withColumn("text", remove_string_udf3(col("text")))
      .withColumn("text", remove_string_udf4(col("text")))

    val Array(train, test) = cleanTweets.randomSplit(Array(4, 1), seed = 104729L)
    val cachedTrain = train.cache()
    val cachedTest = test.cache()

    /*
    val stringIndex = new StringIndexer().setInputCol("airline_sentiment").setOutputCol("category").fit(cleanTweets).transform(cleanTweets)
    val tokenIndex = new Tokenizer().setInputCol("text").setOutputCol("words").transform(stringIndex)
    val removerIndex = new StopWordsRemover().setInputCol("words").setOutputCol("filteredWords").transform(tokenIndex)

    val hashingIndex = new HashingTF().setInputCol("filteredWords").setOutputCol("rawFeatures")//.setNumFeatures(50)
    val featurizedData = hashingIndex.transform(removerIndex)

    val idfIndex = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idfIndex.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData).select("airline", "category", "features").withColumnRenamed("category", "label")

    val Array(train, test) = rescaledData.randomSplit(Array(0.8, 0.2))
    val cachedTrain = train.cache()
    val cachedTest = test.cache()

    */




    val indexer = new StringIndexer().setInputCol("airline_sentiment").setOutputCol("label").fit(cleanTweets)
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filteredWords")
    val hashingTF = new HashingTF().setInputCol("filteredWords").setOutputCol("features")
    // val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features").fit(cleanTweets)


    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setFeaturesCol("features")
      .setLabelCol("label")

    val pipeline_lr = new Pipeline().setStages(Array(indexer, tokenizer, remover, hashingTF, lr))

    val paramGrid_lr = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.elasticNetParam, Array(0.2, 0.8))
      .build()

    val cv_lr = new CrossValidator()
      .setEstimator(pipeline_lr)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid_lr)
      .setNumFolds(3)  // Use 3+ in practice

    // Run cross-validation, and choose the best set of parameters.
    val cvModel_lr = cv_lr.fit(cachedTrain)
    val cvPredict_lr = cvModel_lr.transform(cachedTest).select("label","prediction")

    val evaluator = new MulticlassClassificationEvaluator()
    evaluator.setLabelCol("label")
    evaluator.setMetricName("accuracy")

    val accuracy = evaluator.evaluate(cvPredict_lr)
    val accuracyPrint = "Test Accuracy of logistic regression = %.2f%%".format(accuracy*100)

    evaluator.setMetricName("f1")
    val f1 = evaluator.evaluate(cvPredict_lr)
    val f1Print = "Test f1 measure of logistic regression = %.2f%%".format(f1*100)

    evaluator.setMetricName("weightedPrecision")
    val weightedPrecision = evaluator.evaluate(cvPredict_lr)
    val precisionPrint = "Test precision of logistic regression = %.2f%%".format(weightedPrecision*100)

    evaluator.setMetricName("weightedRecall")
    val weightedRecall = evaluator.evaluate(cvPredict_lr)
    val recallPrint = "Test recall of logistic regression  = %.2f%%".format(weightedRecall*100)

    val printlnString_lr = accuracyPrint + "\n" + f1Print + "\n"  + precisionPrint + "\n" + recallPrint + "\n"







    // Decision Tree model

    val algTree = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setImpurity("gini")

    val pipeline_tree = new Pipeline().setStages(Array(indexer, tokenizer, remover, hashingTF, algTree))


    val paramGrid_tree = new ParamGridBuilder()
      .addGrid(algTree.maxDepth, Array(2, 4))
      .build()

    val cv_tree = new CrossValidator()
      .setEstimator(pipeline_tree)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid_tree)
      .setNumFolds(3)  // Use 3+ in practice

    // Run cross-validation, and choose the best set of parameters.
    val cvModel_tree = cv_tree.fit(cachedTrain.limit(200))

    val cvPredict_tree = cvModel_tree.transform(cachedTest.limit(40)).select("label","prediction")

    val evaluator_tree = new MulticlassClassificationEvaluator()
    evaluator_tree.setLabelCol("label")
    evaluator_tree.setMetricName("accuracy")

    val accuracy_tree = evaluator_tree.evaluate(cvPredict_tree)
    val accuracyPrint_tree = "Test Accuracy of decision tree = %.2f%%".format(accuracy_tree*100)

    evaluator_tree.setMetricName("f1")
    val f1_tree = evaluator_tree.evaluate(cvPredict_tree)
    val f1Print_tree = "Test f1 measure of decision tree = %.2f%%".format(f1_tree*100)

    evaluator_tree.setMetricName("weightedPrecision")
    val weightedPrecision_tree = evaluator_tree.evaluate(cvPredict_tree)
    val precisionPrint_tree = "Test precision of decision tree = %.2f%%".format(weightedPrecision_tree*100)

    evaluator_tree.setMetricName("weightedRecall")
    val weightedRecall_tree = evaluator_tree.evaluate(cvPredict_tree)
    val recallPrint_tree = "Test recall of decision tree  = %.2f%%".format(weightedRecall_tree*100)




    // setup print out
    val printraw = "Total raw data records: " + rawTweets.count() + "\n"
    val printNotNull = "Total data records after removing null: " + noNullTweets.count() + "\n"
    val printcount = "80/20 split: Train = " + cachedTrain.count() + " Test = " + cachedTest.count() + "\n"
    val printlnString_tree = accuracyPrint_tree + "\n" + f1Print_tree + "\n"  + precisionPrint_tree + "\n" + recallPrint_tree + "\n"
    val printResult = printraw + printNotNull + "\n" + printcount + "\n" + printlnString_lr + "\n" + printlnString_tree

    val result = spark.sparkContext.parallelize(Seq(printResult))
    result.saveAsTextFile(args(1))

  }
}