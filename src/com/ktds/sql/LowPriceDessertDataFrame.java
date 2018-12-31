package com.ktds.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class LowPriceDessertDataFrame {

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
		
		dessertDataframe.select( dessertDataframe.col("name"), dessertDataframe.col("price").as("PRICE") )
//						.where("price >= 4000")
//						.where("price < 5000")
						.where( dessertDataframe.col("price")
												.divide(1000)
												.cast(DataTypes.IntegerType)
												.multiply(1000)
												.$eq$eq$eq(4000) )
						.orderBy( dessertDataframe.col("price").desc() )
						.show(30);
		
		session.close();
		sc.close();
		
	}
	
}
