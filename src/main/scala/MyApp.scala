
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

object MyApp {

  def main(args: Array[String]): Unit = {

    val zkQuorum = args(0)
    val group = args(2)
    val topics = args(1)
    val numThreads = 1
    val sparkConf = new SparkConf().setAppName("Twitter Tags").setMaster("yarn-client")
    val sc = new org.apache.spark.SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val tweets = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val hashTags = tweets.flatMap(_.split(" ")).filter(_.startsWith("#"))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(30))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    // Print popular hashtags
    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(30)
      println("\nPopular in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })


    ssc.start()
    ssc.awaitTermination()

  }
}
