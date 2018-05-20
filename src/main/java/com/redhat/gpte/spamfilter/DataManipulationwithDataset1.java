package com.redhat.gpte.spamfilter;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

public class DataManipulationwithDataset1 {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("DatasetDemo")
				 .master("local[*]")
				.config("spark.sql.warehouse.dir", "/tmp/").getOrCreate();
		Dataset<Row> df = spark.read().text("input/SMSSpamCollection.txt");
		df.show();

		//transfer it to RDD and do map transformation
		JavaRDD<Row> rowRDD = df.toJavaRDD();
		
		//System.out.println(rowRDD.first());
		JavaRDD<Row> splitedRDD = rowRDD.map(r -> {
			// TODO Auto-generated method stub
			String[] split = r.getString(0).split("\t");
			return RowFactory.create(split[0],split[1]);
		});
		List<StructField> fields = new ArrayList<>();
		fields.add(DataTypes.createStructField("labelString", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("featureString", DataTypes.StringType, true));
		org.apache.spark.sql.types.StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> schemaSMSSpamCollection = spark.sqlContext().createDataFrame(splitedRDD, schema);
		schemaSMSSpamCollection.printSchema();
		
		/*
		 * adding new columns
		 */
		fields.add(DataTypes.createStructField("labelDouble", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("featureTokens", DataTypes.StringType, true));
		org.apache.spark.sql.types.StructType schemaUpdated = DataTypes.createStructType(fields);

		Dataset<Row> newColumnsAddedDF = spark.sqlContext().createDataFrame(schemaSMSSpamCollection.javaRDD().map(row -> {
			// TODO Auto-generated method stub
			double label;
			if(row.getString(0).equalsIgnoreCase("spam"))
				label = 1.0;
			else
				label = 0.0;
			String[] split = row.getString(1).split(" ");
			ArrayList<String> tokens = new ArrayList<>();
			for(String s:split)
				tokens.add(s.trim());
			return RowFactory.create(row.getString(0),row.getString(1),label, tokens.toString());
		}), schemaUpdated);
		
		newColumnsAddedDF.show();
		newColumnsAddedDF.select(newColumnsAddedDF.col("labelDouble"), newColumnsAddedDF.col("featureTokens")).show();
		newColumnsAddedDF.filter(newColumnsAddedDF.col("labelDouble").gt(0.0)).show();
		newColumnsAddedDF.groupBy("labelDouble").count().show();
		
		newColumnsAddedDF.createOrReplaceTempView("SMSSpamCollection");
		Dataset<Row> spam = spark.sqlContext().sql("SELECT * FROM SMSSpamCollection WHERE labelDouble=1.0");
		spam.show();
		
		Dataset<Row> counts = spark.sqlContext().sql("SELECT labelDouble, COUNT(*) AS count FROM SMSSpamCollection GROUP BY labelDouble");
		counts.show();
		
		JavaRDD<SMSSpamBean> smsSpamBeanRDD =  rowRDD.map(r -> {
			String[] split = r.getString(0).split("\t");
			return new SMSSpamBean(split[0],split[1]);
		});
		
		Dataset<Row> SMSSpamDF = spark.sqlContext().createDataFrame(smsSpamBeanRDD, SMSSpamBean.class);
		SMSSpamDF.show();		
	}
}
