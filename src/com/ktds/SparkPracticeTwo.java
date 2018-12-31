package com.ktds;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkPracticeTwo {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
				.setAppName("Spark Test")
				.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath ="C:\\Users\\KKJ\\Desktop\\data\\2M.ID.CONTENTS";
		String savePath="C:\\Users\\KKJ\\Desktop\\data\\SparkPractice";
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
		
		rdd.map(data -> data.split("\t"))
        .filter(array -> array.length > 1)
        .map(array -> array[1].split("[^a-zA-Z0-9]+"))
        .flatMapToPair(array -> Arrays.asList(array)
                               .parallelStream()
                               .filter(word -> word.trim().length() > 0)
                               .map(word -> new Tuple2<>(word, 1))
                               .collect(Collectors.toList())
                               .iterator()
        )
        .reduceByKey((amount, value) -> amount+value )
        .filter(tuple -> tuple._2 > 10000)
        .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
        .repartition(1)
        .sortByKey(false)
        .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
        .saveAsTextFile(savePath);
		   
		sc.close();
		
	}

}
