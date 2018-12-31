package com.ktds.streaming;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

public class ProjectRealTimeLogAnalysis {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("projectRealTimeLogAnalysis")
										.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(10));
		
		Map<String, Object> kafkaConf = new HashMap<>();
		kafkaConf.put("group.id", "kafka-consumer-group");
		kafkaConf.put("bootstrap.servers", "localhost:9092");
		kafkaConf.put("value.deserializer", StringDeserializer.class);
		kafkaConf.put("key.deserializer", StringDeserializer.class);
		
		List<String> topic = new ArrayList<>();
		topic.add("SparkTopic");
		
		JavaInputDStream<ConsumerRecord<String,String>> kafkaDataStream = 
			KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent()
					, ConsumerStrategies.Subscribe(topic, kafkaConf));
		
		kafkaDataStream.map( (data) -> data.value().toString() )
						.map( (data) -> data.split("#"))
						.mapToPair( (arr) -> {
							String[] dataArray = new String[3];
							String[] infoArray = arr[0].split(" ");
							dataArray[0] = infoArray[0];
							dataArray[1] = arr[3];
							dataArray[2] = arr[4];
							return new Tuple2<>(new Tuple3<>(dataArray[0], dataArray[1], dataArray[2]),1);
						})
						.reduceByKey( (amount,value) -> amount + value )
						.map( (tuple) -> {
							String data = tuple._1._1() + "ł" + tuple._1._2() + "ł" + tuple._1._3() + "ł" + tuple._2;
							return data;
						})
						.foreachRDD( (data) -> data.foreach( (rddData) -> {
								KafkaSender.send(rddData);
							})
						);
		
		kafkaDataStream.map( (data) -> data.value().toString() )
						.map( (data) -> data.split("#"))
						.filter( (arr) -> arr[2].length() > 0 )
						.mapToPair ( (arr) -> {
							String[] dataArray = new String[5];
							String[] dateArray = arr[0].split(" ");
							dataArray[0] = arr[1];
							dataArray[1] = arr[2];
							dataArray[2] = dateArray[0];
							dataArray[3] = arr[3];
							dataArray[4] = arr[4];
							return new Tuple2<>(new Tuple5<>(dataArray[0], dataArray[1], dataArray[2], dataArray[3], dataArray[4]),1);
						})
						.reduceByKey( (amount,key) -> amount + key )
						.map( (tuple) -> {
							String data = tuple._1._1() + "ł" + tuple._1._2() + "ł" + tuple._1._3() + "ł" + tuple._1._4() + "ł" + tuple._1._5() + "ł" + tuple._2 ;
							return data;
						} )
						.foreachRDD( (data) -> data.foreach( (rddData) -> {
							KafkaSender.send("IDTopic", rddData);
						}));
		
		kafkaDataStream.map( (data) -> data.value().toString() )
						.window( Durations.minutes(5) )
						.map( (data) -> data.split("#"))
						.mapToPair( (arr) -> {
							String[] dateArray = arr[0].split(":");
							String date = dateArray[0] + ":" + dateArray[1];							
							return new Tuple2<>(new Tuple2<>(date,arr[3]),1);
						})
						.reduceByKey( (amount, value) -> amount + value )
						.map( (tuple) -> {
							String data = tuple._1._1() + "ł" + tuple._1._2() + "ł" + tuple._2;
							return data;
						})
						.foreachRDD( (data) -> data.foreach( (rddData) ->{
							KafkaSender.send("RealTimeTopic", rddData);
						}
						));
		
		ssc.start();
		
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			ssc.close();
			e.printStackTrace();
		}
	}
	
}
