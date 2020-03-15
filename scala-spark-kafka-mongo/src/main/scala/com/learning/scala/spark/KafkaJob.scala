package com.learning.scala.spark

import java.util.UUID

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import com.learning.scala.spark.model.Header
import com.mongodb.spark.MongoSpark

case class Row(header: Header, body: String)

object KafkaJob {

	@transient private var instance: SQLContext = null;

	def getInstance(sparkContext: SparkContext): SQLContext = synchronized {
		if (instance == null) {
			instance = new SQLContext(sparkContext);
		}
		instance;
	}

	def main(args: Array[String]) {
	  
		val conf = new SparkConf().setAppName("SparkKafkaStreaming").setMaster("local[1]");
		val ssc = new StreamingContext(conf, Seconds(1));

		val spark = SparkSession.builder()
				.master("local[1]")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/")
				.config("spark.mongodb.output.database", "admin")
				.config("spark.mongodb.output.collection", "sparktest")
				.getOrCreate();

		val kafkaParams = Map[String, Object](
				"bootstrap.servers" -> "localhost:9092",
				"key.deserializer" -> classOf[StringDeserializer],
				"value.deserializer" -> classOf[StringDeserializer],
				"group.id" -> "use_a_separate_group_id_for_each_stream",
				"auto.offset.reset" -> "latest",
				"enable.auto.commit" -> (true: java.lang.Boolean)
				);

		val topics = Array("test", "test2");
		val stream = KafkaUtils.createDirectStream[String, String](
				ssc,
				PreferConsistent,
				Subscribe[String, String](topics, kafkaParams)
				);

		val records = stream.map(record => (record.key, record.value));

		records.foreachRDD(rdd => {
			val sqlContext = KafkaJob.getInstance(rdd.sparkContext);
			import sqlContext.implicits._;
			val dataFrame = rdd.map({case (rdd) => Row(new Header(UUID.randomUUID().toString()), rdd._2)}).toDF();
			dataFrame.show();
			MongoSpark.save(dataFrame.write.format("com.mongodb.spark.sql").mode("append"));
		});

		ssc.start();
		ssc.awaitTermination();

	}
	
}