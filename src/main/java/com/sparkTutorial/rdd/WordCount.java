package com.sparkTutorial.rdd;

import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WordCount {

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession spark = SparkSession.builder().appName("wordCounts").config("spark.sql.warehouse.dir", "out")
				.getOrCreate();

		JavaRDD<String> lines = sc.textFile("in/word_count.text");
		JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

		Encoder<String> stringEncoder = Encoders.STRING();
		Dataset<Row> test = spark.createDataset(words.rdd(), stringEncoder).toDF();
		test.show();
		Map<String, Long> wordCounts = words.countByValue();

		for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}
		JavaRDD<String> linesWithOf = words.filter(word -> word.contains("of"));
		System.out.println(linesWithOf.count());
	}
}
