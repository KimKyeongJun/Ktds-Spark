package com.ktds.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DessertSparkTest {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
								.setAppName("Dessert")
								.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String fileName = "C:\\Users\\KKJ\\Desktop\\data\\dessert-menu.csv";
		
		JavaRDD<Dessert> dessert = sc.textFile(fileName)
									 .filter( (line) -> line.trim().length() > 0)
									 .map( (line)-> new Dessert(line.split(",")) );
		
		// SpartSQL을 사용하기 위한 Session 생성
		SparkSession session = SparkSession.builder()
											.appName("DessertSql")
											.master("local[*]")
											.getOrCreate();
		
		// session을 이용해 dessert를 DB화
		Dataset<Row> dessertDataframe = session.createDataFrame(dessert, Dessert.class);
		
		dessertDataframe.createOrReplaceTempView("dessert_table");
		
		
		StringBuffer query = new StringBuffer();
		
		query.append(" SELECT	COUNT(*) AS NUM_OF_OVER_300KCAL ");
		query.append(" FROM	DESSERT_TABLE                       ");
		query.append(" WHERE	KCAL > 260                      ");
		
		Dataset<Row> numOfOver300kcal = session.sql(query.toString());
		
		query = new StringBuffer();
		
		query.append(" SELECT	COUNT(*) AS CNT, KCAL ");
		query.append(" FROM		DESSERT_TABLE ");
		query.append(" GROUP	BY KCAL ");
		query.append(" ORDER	BY CNT DESC ");
		query.append(" 			, KCAL ");
		
		Dataset<Row> groupDessert = session.sql(query.toString());
		
		//출력
		numOfOver300kcal.show(30);
		groupDessert.show(30);
		
		
		session.close();
		sc.close();
		
	}

}
