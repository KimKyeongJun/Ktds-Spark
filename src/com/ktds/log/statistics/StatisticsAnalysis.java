package com.ktds.log.statistics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.ktds.log.statistics.dao.StatisticsDao;

public class StatisticsAnalysis {
	
	//Spring DI를 해주는 DI 전용 클래스
	private static AbstractXmlApplicationContext ctx;
	
	private static StatisticsDao getDao() {
		if ( ctx == null ) {
			//DispathcherServlet or ContextLoader 역할
			ctx = new ClassPathXmlApplicationContext("classpath:config/rootContext.xml", "classpath:config/applicationContext.xml");
		}
		
		return ctx.getBean("statisticsDao", StatisticsDao.class);
	}
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
							.setAppName("Statistics")
							.setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String fileName = "D:\\statistics.2018-08-30.log";
		
		JavaRDD<Statistics> rdd = sc.textFile(fileName)
									.map( (line) -> {
										
										String[] resultArray = new String[5];
										String[] array = line.split(",");
										
										String[] dateArray = array[0].split(":");
										resultArray[0] = dateArray[2].trim();
										resultArray[1] = dateArray[3].trim();
										resultArray[2] = dateArray[4].split("\\.")[0].trim();
										
										String url = array[1].split(":")[1].trim();
										
										resultArray[3] = url;
										
										String ip = array[2].replace("IP :", "");
										resultArray[4] = ip.trim();
										
										return new Statistics(resultArray);
										
									} );
		
		SparkSession session = SparkSession.builder()
											.appName("Statistics SQL")
											.master("local[*]")
											.getOrCreate();
		
		
		session.createDataFrame(rdd, Statistics.class)
				.groupBy("hour","minite","second", "url", "ip")
				.agg(functions.count("*").as("req_count"))
				.toJavaRDD()
				.map( (row) -> {
					String hour = row.getAs("hour");
					String minite = row.getAs("minite");
					String second = row.getAs("second");
					String url = row.getAs("url");
					String ip = row.getAs("ip");
					int req_count = Math.toIntExact( row.getAs("req_count") );
					return new Statistics(hour, minite, second, url, ip, req_count);
				})
				.foreach( (statistics) -> StatisticsAnalysis.getDao().insertStatisticsBySeconds(statistics) );
		
		session.createDataFrame(rdd, Statistics.class)
			.groupBy("hour","minite","url", "ip")
				.agg(functions.count("*").as("req_count"))
				.toJavaRDD()
				.map( (row) -> {
					String hour = row.getAs("hour");
					String minite = row.getAs("minite");
					String url = row.getAs("url");
					String ip = row.getAs("ip");
					int req_count = Math.toIntExact( row.getAs("req_count") );
					return new Statistics(hour, minite, url, ip, req_count);
				})
				.foreach( (statistics) -> StatisticsAnalysis.getDao().insertStatisticsByMinutes(statistics) );
		
		session.createDataFrame(rdd, Statistics.class)
				.groupBy("hour", "url", "ip")
				.agg(functions.count("*").as("req_count"))
				.toJavaRDD()
				.map( (row) -> {
					String hour = row.getAs("hour");
					String url = row.getAs("url");
					String ip = row.getAs("ip");
					int req_count = Math.toIntExact( row.getAs("req_count") );
					return new Statistics(hour, url, ip, req_count);
				})
				.foreach( (statistics) -> StatisticsAnalysis.getDao().insertStatisticsByHours(statistics) );
		
		
		session.close();
		sc.close();
	}

}
