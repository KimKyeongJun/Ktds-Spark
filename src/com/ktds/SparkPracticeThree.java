package com.ktds;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class SparkPracticeThree {
	
	public static void main(String[] args) throws IOException {
		
		List<String> newsLines = Files.readAllLines(Paths.get("C:\\Users\\KKJ\\Desktop\\data","News2.txt"));
		/*
		newsLines.stream()
				.filter(line -> line.length() > 0)
				.map( (line) -> Arrays.stream( line.split("^a-zA-Z0-9가힣") )
											.map( (word) -> word.trim())
											.filter( (word) -> word.length() > 0)
											.collect(Collectors.toList())
				)
				.flatMap( list -> {
					List<String> tigram = new ArrayList<>();
					String word;
					for ( int i=0; i<list.size()-2; i++ ) {
						word = list.get(i) + " " + list.get(i+1) + " " + list.get(i+2);
						trigram.add(word);
					}
					
					return tiagram.stream();
				})
				.forEach(System.out::println);
		*/
	}

}
