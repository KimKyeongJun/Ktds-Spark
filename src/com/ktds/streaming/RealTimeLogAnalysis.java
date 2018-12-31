package com.ktds.streaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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


public class RealTimeLogAnalysis {
	
	

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
										.setAppName("RealTimeLogAnalysis")
										.setMaster("local[*]");
		
		// 정적 파일을 분석할 때 많이 사용한다. (실시간 처리 못함)
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		// Spark Stream 전용 객체 생성		
		JavaStreamingContext ssc = new JavaStreamingContext( sc, Durations.seconds(10) );
		
		// Kafka 연결
		Map<String, Object> kafkaConf = new HashMap<>();
		//kafkaConf.put("metadata.broker.list", "localhost:9092");
		kafkaConf.put("group.id",  "kafka-consumer-group");
		kafkaConf.put("bootstrap.servers",  "localhost:9092");
		kafkaConf.put("value.deserializer",  StringDeserializer.class);		//직렬화된 Data를 받아온다.(Kafka에서)
		kafkaConf.put("key.deserializer",  StringDeserializer.class);
		
		// Topic 정의 (여러개의 Topic 가져올 수 있다)
		List<String> topic = new ArrayList<>();
		topic.add("logTopic");
		
		// Kafka Data 받아오기 (실시간 Data)
		JavaInputDStream<ConsumerRecord<String, String>> kafkaDataStream = 
		KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent()
				, ConsumerStrategies.Subscribe(topic, kafkaConf));
		
		//#YYYY:mm:DD:HH:MM:SS#IP#URL#GETorPOST
		kafkaDataStream.map( (record) -> record.value().toString() )
						.filter( (log) -> log.startsWith("#") )
						.map( (log) -> log.split("#") )
						.map( (logArray) -> Arrays.stream(logArray)
													.skip(1)
													.collect(Collectors.toList())
													.toArray(new String[4]) )
						.map( (logArray) ->  {
							String[] dateTimeArray = logArray[0].split(":");
							
							List<String> dataList = new ArrayList<>();
							dataList.add(dateTimeArray[0]);		//YYYY
							dataList.add(dateTimeArray[1]);		//MM
							dataList.add(dateTimeArray[2]);		//DD
							dataList.add(dateTimeArray[3]);		//HH
							dataList.add(dateTimeArray[4]);		//MM
							dataList.add(dateTimeArray[5]);		//SS
							dataList.add(logArray[1]);			//IP
							dataList.add(logArray[2]);			//URL
							dataList.add(logArray[3]);			//METHOD
							
							
							return dataList.stream().collect(Collectors.joining("ł"));
						} )
						.foreachRDD( (rdd) -> {
							rdd.foreach( (log) -> {
								KafkaSender.send("SPARK " + log);		//Batch로 보내는 Code
							});
						});
		
		ssc.start();
		
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			ssc.close();
			e.printStackTrace();
		}
		
	}
	
}
