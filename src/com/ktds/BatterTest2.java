package com.ktds;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class BatterTest2 {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
				.setAppName("Spark Test")
				.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath ="C:\\Users\\KKJ\\Desktop\\data\\10K.ID.CONTENTS";
		JavaRDD<String> rdd = sc.textFile(filePath);
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
