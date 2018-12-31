package com.ktds;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkPracticeFour {

	public static void main(String[] args) throws IOException {

		SparkConf conf = new SparkConf().setAppName("Spark Test").setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);

		String filePath = "C:\\Users\\KKJ\\Desktop\\data\\10K.ID.CONTENTS";

		/*sc.textFile(filePath)
				.map(line -> line.split("\t"))
				.filter(array -> array.length > 1)
				.map(array -> array[1].split("[^a-zA-Z0-9]+"))
				.map(array -> {
					List<String> wordTrigram = new ArrayList<>();
					String word;
					for (int i = 0; i < array.length; i++) {
						word = array[i] + " ";
						if ((i + 2) <= array.length - 1) {
							word += array[i + 1] + " ";
							word += array[i + 2];
							wordTrigram.add(word);
						}
					}
					return wordTrigram;
				})
				.flatMapToPair(list -> list.parallelStream().filter(word -> word.length() > 0)
						.filter(word -> word.split(" ").length == 3).map(word -> new Tuple2<>(word.toLowerCase(), 1))
						.collect(Collectors.toList()).iterator())
				.reduceByKey((amount, count) -> amount + count).filter(tuple -> tuple._2 > 100)
				.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1)).repartition(1).sortByKey(false)
				.map(tuple -> new Tuple2<>(tuple._2, tuple._1)).foreach(tuple -> System.out.println(tuple));
*/
		sc.close();
	}

}
