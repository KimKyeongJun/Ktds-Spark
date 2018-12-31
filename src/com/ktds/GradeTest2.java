package com.ktds;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class GradeTest2 {
	
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
		rdd.filter( line -> !line.contains("Java,Database,"))
		   .map( (line) -> line.split(","))
		   .mapToPair( array -> {
			   String name = array[0];
			   int average = (int) Arrays.stream(array)
					   			.skip(1)
					   			.mapToInt( score -> Integer.parseInt(score.trim()))
					   			.average()
					   			.orElse(0);
			   return new Tuple2<>(average, name);
		   })
		   .repartition(1)
		   .sortByKey(false)
		   .map( tuple -> new Tuple2<>(tuple._2, tuple._1))
		   .foreach(tuple -> System.out.println(tuple));
		
		sc.close();
		
	}

}
