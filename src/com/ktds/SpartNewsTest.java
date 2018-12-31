package com.ktds;



import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SpartNewsTest {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
				.setAppName("Spark Test")
				.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath ="C:\\Users\\KKJ\\Desktop\\data\\News.txt";
		String savePath ="C:\\Users\\KKJ\\Desktop\\data\\News_Count";
		JavaRDD<String> rdd = sc.textFile(filePath);
		rdd.filter( (line) -> line.trim().length() > 0 )
		   .flatMap( (line) -> Arrays.asList(line.split(" ")).iterator() )
		   .mapToPair( (word) -> new Tuple2<>(word,1) )
		   .reduceByKey( (amount, value) -> amount+ value)
		   .mapToPair( (tuple) -> new Tuple2<>(tuple._2,tuple._1))
		   .repartition(1)
		   .sortByKey(false)
		   .saveAsTextFile(savePath);
		
		sc.close();
		
	}

}
