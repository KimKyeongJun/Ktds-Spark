package com.ktds;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkTest {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
				.setAppName("Spark Test")
				.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String filePath ="C:\\Users\\KKJ\\Desktop\\data\\text.txt";	
		JavaRDD<String> rdd = sc.textFile(filePath);
		rdd
		   //.filter( (str) -> !str.equalsIgnoreCase("cat") )
		   //.map( (str) -> str.toUpperCase() )
		   //.map( (str) -> str.replace(".", "")
			//	   			 .replace("&", ""))
		   //.map( (str) -> str + " (" + str.length() + ")" )
		   //.foreach( (str) -> System.out.println(str) );
		   .mapToPair( (str) -> new Tuple2<>(str,1) )
		   .reduceByKey( (amount, value) -> amount + value)
		   .foreach( (map) -> System.out.println(map));
		
		
		sc.close();
		
	}

}
