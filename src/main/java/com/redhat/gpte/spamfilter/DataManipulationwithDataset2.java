package com.redhat.gpte.spamfilter;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataManipulationwithDataset2 implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static SparkSession spark = SparkSession.builder()
			.appName("DatasetDemo")
			.master("local[*]")
			.config("spark.sql.warehouse.dir", "/tmp/")
			.getOrCreate();

	public static void main(String[] args) {
		Dataset<String> ds = spark.read().text("input/SMSSpamCollection.txt").as(org.apache.spark.sql.Encoders.STRING());
		ds.show();
		
		Dataset<SMSSpamTokenizedBean> dsSMSSpam = ds.map(
				value -> {
					String[] split = value.split("\t");
					double label;
					if(split[0].equalsIgnoreCase("spam"))
						label = 1.0;
					else
						label=0.0;
					ArrayList<String> tokens = new ArrayList<>();
					for(String s:split)
						tokens.add(s.trim());
							
					return new SMSSpamTokenizedBean(label, tokens.toString());
				}, org.apache.spark.sql.Encoders.bean(SMSSpamTokenizedBean.class));
		dsSMSSpam.show();
		dsSMSSpam.printSchema();
		Dataset<Row> df = dsSMSSpam.toDF();
		df.createOrReplaceTempView("SMSSpamCollection");
		df.show();
	}
}