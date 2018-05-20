package com.redhat.gpte.spamfilter;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class DataManipulatiuonsWithRDD {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("SpamFilterDataset").master("local[*]")
				.config("spark.sql.warehouse.dir", "/tmp/").getOrCreate();
		String path = "input/SMSSpamCollection.txt";
		RDD<String> lines = spark.sparkContext().textFile(path, 2);
		System.out.println(lines.take(10));

		JavaRDD<String> spamRDD = lines.toJavaRDD().filter(line -> line.split("\t")[0].equals("spam"));
		JavaRDD<String> spam = spamRDD.map(line -> line.split("\t")[1]);
		spam.saveAsTextFile("output/spam");
		System.out.println(spam.collect());

		JavaRDD<String> hamRDD = lines.toJavaRDD().filter(line -> line.split("\t")[0].equals("ham"));
		JavaRDD<String> ham = hamRDD.map(line -> line.split("\t")[1]);
		ham.saveAsTextFile("output/ham");
		
		JavaRDD<String> spam_ham = spam.union(ham);
		spam_ham.saveAsTextFile("models/spam_ham.txt");
		
		JavaRDD<ArrayList<String>> spamWordList = spam
				.map(line -> {
					// TODO Auto-generated method stub
					ArrayList<String> words = new ArrayList<>();
					words.addAll(Arrays.asList(line.split(" ")));
					return words;
				});
		JavaRDD<Tuple2<Double, ArrayList<String>>> spamWordsLabelPair2 = spamWordList
				.map(v1 -> new Tuple2<Double, ArrayList<String>>(1.0, v1));
		for (Tuple2<Double, ArrayList<String>> tt : spamWordsLabelPair2
				.collect()) {
			System.out.println(tt._1 + ": " + tt._2.toString());
		}
		JavaRDD<ArrayList<String>> hamWordList = ham
				.map(line -> {
					// TODO Auto-generated method stub
					ArrayList<String> words = new ArrayList<>();
					words.addAll(Arrays.asList(line.split(" ")));
					return words;
				});

		JavaRDD<Tuple2<Double, ArrayList<String>>> hamWordsLabelPair2 = hamWordList
				.map(v1 -> new Tuple2<Double, ArrayList<String>>(0.0, v1));

		for (Tuple2<Double, ArrayList<String>> tt : hamWordsLabelPair2
				.collect()) {
			System.out.println(tt._1 + ": " + tt._2.toString());
		}

		// Union
		JavaRDD<Tuple2<Double, ArrayList<String>>> train_set = spamWordsLabelPair2
				.union(hamWordsLabelPair2);
		for (Tuple2<Double, ArrayList<String>> tt : train_set.take(10)) {
			System.out.println(tt._1 + ": " + tt._2.toString());
		}
		System.out.println(spamWordsLabelPair2.count());
		System.out.println(hamWordsLabelPair2.count());
		System.out.println(train_set.count());
		//JavaRDD<String> spam_ham1 = spam.union(ham);
		train_set.saveAsTextFile("output/train_set.txt");
		SparkSession.clearActiveSession();
	}
}
