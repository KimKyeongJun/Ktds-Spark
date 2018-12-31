package com.ktds;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class GradeTest {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
				.setAppName("Spark Test")
				.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath ="C:\\Users\\KKJ\\Desktop\\data\\성적.csv";
		JavaRDD<String> rdd = sc.textFile(filePath);
		//title 건너뛰기
		//String title = rdd.first();
		//System.out.println(title);
		/*
		rdd.filter( line -> !line.contains("Java,Database,"))
		   .map( (line) -> line.split(","))
		   .map( (line) -> new Tuple2<>(line[0], Integer.parseInt(line[1])+Integer.parseInt(line[2])+Integer.parseInt(line[3])+Integer.parseInt(line[4])
		   +Integer.parseInt(line[5])+Integer.parseInt(line[6])+Integer.parseInt(line[7])))
		   .map( (map) -> new Tuple2<>(map._1,(double)map._2/7))
		   .mapToPair( (map) -> new Tuple2<>(map._2,map._1))
		   .repartition(1)
		   .sortByKey(false)
		   //.forEach(System.out::println);
		   .foreach( (line) -> System.out.println(line));
		*/
		
		rdd.filter( line -> !line.contains("Java,Database,"))
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
		
		sc.close();
		
	}

}
