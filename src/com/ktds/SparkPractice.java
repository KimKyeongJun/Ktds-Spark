package com.ktds;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkPractice {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
				.setAppName("Spark Test")
				.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath ="C:\\Users\\KKJ\\Desktop\\data\\10K.ID.CONTENTS";
		JavaRDD<String> rdd = sc.textFile(filePath);
		/*
		rdd.map( (line) -> line.split("\r\n") )
		   .flatMap( (arr) -> Arrays.stream(arr)
				   					.map( (data) -> data.split("\t") )
				   					.filter((data) -> data.length > 1)
				   					.map( (data) -> data[0] +","+ data[1].trim().length() )
				   					.collect(Collectors.toList())
				   					.iterator())
		   .map( (count) -> count.split(","))
		   .mapToPair( (countArr) -> new Tuple2<>(countArr[0], Integer.parseInt(countArr[1])) )
		   .repartition(1)
		   .mapToPair( (map) -> new Tuple2<>(map._2, map._1) )
		   .sortByKey(false)
		   .mapToPair( (map) -> new Tuple2<>(map._2, map._1) )
		   .foreach((line) -> System.out.println(line) );
		*/
		
		rdd.map( (line) -> line.split("[^a-zA-Z0-9]+") )
		   .flatMap( (arr) -> Arrays.stream(arr)
				   				.map( (data) -> data.toLowerCase().trim() + "," + 1 )
				   				.collect( Collectors.toList() )
				   				.iterator()
				   )
		   .map( (count) -> count.split(",") )
		   .mapToPair( (countArr) -> new Tuple2<>(countArr[0], Integer.parseInt(countArr[1])) )
		   .repartition(1)
		   .reduceByKey( (amount, value) -> amount + value)
		   .mapToPair( (map) -> new Tuple2<>(map._2, map._1) )
		   .sortByKey(false)
		   .mapToPair( (map) -> new Tuple2<>(map._2, map._1) )
		   .foreach( (line) -> System.out.println(line) );
		   
		sc.close();
		
	}

}
