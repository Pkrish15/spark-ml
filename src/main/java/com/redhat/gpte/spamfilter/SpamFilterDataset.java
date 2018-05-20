package com.redhat.gpte.spamfilter;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

public class SpamFilterDataset {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
		.builder()
		.appName("SpamFilterDataset")
		.master("local[*]")
		.config("spark.sql.warehouse.dir", "/tmp/")
		.getOrCreate();

		String path = "input/SMSSpamCollection.txt";
		
		RDD<String> lines = spark.sparkContext().textFile(path, 2);
		// System.out.println(lines.take(10));

		JavaRDD<Row> rowRDD = lines.toJavaRDD().map( line -> RowFactory.create(line));
		//System.out.println(rowRDD.collect());
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("line", DataTypes.StringType, true));
		//StructType
		org.apache.spark.sql.types.StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema);
		df.select("line").show();
		Dataset<Row> spam = df.filter(df.col("line").like("%spam%"));
		Dataset<Row> ham = df.filter(df.col("line").like("%ham%"));
//		// Counts all the errors
		System.out.println(spam.count());
		System.out.println(ham.count());
		spam.show();
	}
}
