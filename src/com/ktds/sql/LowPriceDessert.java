package com.ktds.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple4;

public class LowPriceDessert {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
								.setAppName("Dessert")
								.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String fileName ="C:\\Users\\KKJ\\Desktop\\data\\dessert-menu.csv";
		/*
		JavaRDD<Dessert> rdd = sc.textFile(fileName)
								.filter( (line) -> line.trim().length() > 0)
								.map( (line) -> new Dessert(line.trim().split(",")) );
		*/
		JavaRDD<Dessert> rddOne = sc.textFile(fileName)
				.filter( (line) -> line.trim().length() > 0)
				.map( (line) -> {
					String[] array = line.split(",");
					int kcal = Integer.parseInt(array[2]);
					kcal = kcal/100 * 100;
					array[2] = kcal + "";
					return new Dessert( array );					
				} );
		
		System.out.println("RDD 출력");
		sc.textFile(fileName)
				.filter( (line) -> line.trim().length() > 0 )
				.map( (line) -> line.trim().split(",") )
				.filter( (arr) -> Integer.parseInt(arr[2]) <= 5000 )
				.map( (arr) -> new Tuple4<>(arr[0], arr[1], arr[2], arr[3]))
				.foreach( (map) -> System.out.println(map));
		
		SparkSession session = SparkSession.builder()
											.appName("Dessert SQL")
											.master("local[*]")
											.getOrCreate();
		
		Dataset<Row> dessertDataframe = session.createDataFrame(rddOne, Dessert.class);
		dessertDataframe.createOrReplaceTempView("dessert_table");
		
		StringBuffer query = new StringBuffer();
		
		query.append(" SELECT	KCAL, SUM(PRICE) AS PRICE ");
		query.append(" FROM		dessert_table ");
		query.append(" WHERE	PRICE <=5000 ");
		query.append(" GROUP	BY KCAL ");
		query.append(" ORDER	BY KCAL DESC ");
		
		Dataset<Row> queryResult = session.sql(query.toString());
		
		System.out.println("SparkSql 출력");
		queryResult.show();
		
		session.close();
		sc.close();
		
		
	}
	
}
