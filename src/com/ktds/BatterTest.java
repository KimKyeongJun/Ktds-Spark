package com.ktds;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class BatterTest {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
				.setAppName("Spark Test")
				.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath ="C:\\Users\\KKJ\\Desktop\\baseballdatabank-master\\core//Batting.csv";
		JavaRDD<String> rdd = sc.textFile(filePath);
		/*
		rdd.filter( (line) -> !line.contains("playerID,yearID"))
		   .map( (line) -> line.split(",") )
		   .mapToPair( (totalArr) -> new Tuple2<>(totalArr[0],Integer.parseInt(totalArr[8])+Integer.parseInt(totalArr[9])+Integer.parseInt(totalArr[10])+Integer.parseInt(totalArr[11])))
		   .reduceByKey( (amount, count) -> amount + count )
		   .repartition(1)
		   .mapToPair( (map) -> new Tuple2<>(map._2,map._1))
		   .sortByKey(false)
		   .mapToPair( (map) -> new Tuple2<>(map._2, map._1))
		   .foreach( (line) -> System.out.println(line));
		*/
		/*
		rdd.filter( (line) -> !line.contains("playerID,yearID"))
		   .map( (line) -> line.split(",") )
		   .flatMapToPair( (array) -> Arrays.stream(array)
		   			.skip(8)
		   			.limit(4)
		   			.map( (data) -> data.trim() )
		   			.map( (data) -> new Tuple2<>( array[0], Integer.parseInt(data)) )
		   			.collect( Collectors.toList() )
		   			.iterator()
		   	)		   	
		   .reduceByKey( (amount, count) -> amount + count )
		   .repartition(1)
		   .mapToPair( (map) -> new Tuple2<>(map._2,map._1))
		   .sortByKey(false)
		   .mapToPair( (map) -> new Tuple2<>(map._2, map._1))
		   .foreach( (line) -> System.out.println(line));
		*/
		rdd.filter( (line) -> !line.contains("playerID,yearID") )
		   .map( (line) -> line.split(",") )
		   .filter( (arr) -> arr[1].trim().equals("2017") )
		   .filter( (arr) -> !arr[6].equals("0"))
		   .flatMapToPair( (arr) -> Arrays.stream(arr)
				   						.skip(8)
				   						.limit(4)
				   						.map( (data) -> data.trim() )
				   						.map((data) -> new Tuple2<>( arr[0], (double)Integer.parseInt(data)/ Double.parseDouble(arr[6])) )
				   						.collect( Collectors.toList() )
				   						.iterator()
				   		)
		   .reduceByKey( (amount, count) -> amount+count )
		   .repartition(1)
		   .mapToPair( (map) -> new Tuple2<>(map._2, map._1) )
		   .sortByKey(false)
		   .foreach( (line) -> System.out.println(line) );
		/*rdd.filter( line -> !line.contains("Java,Database,"))
		   .map(line -> line.split(","))
		   .flatMap( array -> Arrays.stream(array)
				   				.skip(1)
		   						.map( (score) -> score.trim() )
		   						.map( (score) -> array[0] + "," + score)
		   						.collect(Collectors.toList())
		   						.iterator()
		   	)
   			.map( (score) -> score.split(","))
   			.mapToPair( (scoreArr) -> new Tuple2<>(scoreArr[0], Integer.parseInt(scoreArr[1])))
   			.reduceByKey( (amount, value) -> amount + value)
   			.mapToPair( (scoreTuple) -> new Tuple2<>(scoreTuple._2/7,scoreTuple._1))
   			.repartition(1)
   			.sortByKey(false)
   			.foreach((map) -> System.out.println(map));
		*/
		sc.close();
		
	}

}
