package com.sparkTutorial.pairRdd.sort;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SortedWordCountProblem {

	/*
	 * Create a Spark program to read the an article from in/word_count.text,
	 * output the number of occurrence of each word in descending order.
	 * 
	 * Sample output:
	 * 
	 * apple : 200 shoes : 193 bag : 176 ...
	 */

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("in/word_count.text");
		JavaRDD<String> wordRdd = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

		JavaPairRDD<String, Integer> wordPairRdd = wordRdd
				.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));

		JavaPairRDD<String, Integer> wordCounts = wordPairRdd.reduceByKey((x, y) -> x + y);

		JavaPairRDD<Integer, String> countToWordParis = wordCounts
				.mapToPair(wordToCount -> new Tuple2<>(wordToCount._2(), wordToCount._1().toLowerCase()));

		JavaPairRDD<Integer, String> sortedWordCount = countToWordParis.sortByKey(false);

		JavaPairRDD<String, Integer> sortedWords = sortedWordCount
				.mapToPair(sortedWord -> new Tuple2<>(sortedWord._2(), sortedWord._1()));

		for (Tuple2<String, Integer> price : sortedWords.collect()) {
			System.out.println(price._1() + " : " + price._2());
		}

	}
}
