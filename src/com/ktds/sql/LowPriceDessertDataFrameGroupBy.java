package com.ktds.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class LowPriceDessertDataFrameGroupBy {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
								.setAppName("Dessert")
								.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String fileName ="C:\\Users\\KKJ\\Desktop\\data\\dessert-menu.csv";
		
		JavaRDD<Dessert> rdd = sc.textFile(fileName)
				.filter( (line) -> line.trim().length() > 0)
				.map( (line) ->  new Dessert( line.split(",")) );
		
		SparkSession session = SparkSession.builder()
											.appName("Dessert SQL")
											.master("local[*]")
											.getOrCreate();
		
		Dataset<Row> dessertDataframe = session.createDataFrame(rdd, Dessert.class);
		
		Dataset<Row> aggResult = dessertDataframe.groupBy( dessertDataframe.col("price")
													.$div(1000)
													.cast("int")
													.$times(1000)
													.as("PRICE_RANGE") )
						.agg( functions.count("price").as("PRICE_COUNT") );
		aggResult.orderBy( aggResult.col("PRICE_RANGE").desc() )
				.show();
		
		session.close();
		sc.close();
		
	}
	
}
