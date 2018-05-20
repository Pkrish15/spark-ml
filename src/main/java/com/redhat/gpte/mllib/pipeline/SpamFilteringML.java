package com.redhat.gpte.mllib.pipeline;

import java.io.Serializable;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class SpamFilteringML implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6427002983067765409L;
	static SparkSession spark = SparkSession.builder().appName("SpamFilteringML").master("local[*]")
			.config("spark.sql.warehouse.dir", "/tmp/").getOrCreate();

	public static void main(String[] args) {
		Dataset<Row> df = spark.read().text("input/SMSSpamCollection.txt");
		df.show();

		// Feature Transformers (RegexTokenizer)
		RegexTokenizer regexTokenizer1 = new RegexTokenizer()
				.setInputCol("value")
				.setOutputCol("labelText")
				.setPattern("\\t.*$");
		
		Dataset<Row> labelTextDataFrame = regexTokenizer1.transform(df);
		RegexTokenizer regexTokenizer2 = new RegexTokenizer()
				.setInputCol("value").setOutputCol("text").setPattern("\\W");
		Dataset<Row> labelFeatureDataFrame = regexTokenizer2
				.transform(labelTextDataFrame);
		for (Row r : labelFeatureDataFrame.select("text", "labelText").collectAsList()) {
			System.out.println( r.getAs(1) + ": " + r.getAs(0));
		}			
		
		Dataset<Row> newDF = labelFeatureDataFrame.withColumn("labelTextTemp",
				labelFeatureDataFrame.col("labelText").cast(DataTypes.StringType))
				.drop(labelFeatureDataFrame.col("labelText"))
				.withColumnRenamed("labelTextTemp", "labelText");
		
		// Feature Transformer (StringIndexer)
		StringIndexer indexer = new StringIndexer().setInputCol("labelText")
				.setOutputCol("label");
		Dataset<Row> indexed = indexer.fit(newDF).transform(newDF);
		indexed.select(indexed.col("labelText"), indexed.col("label"), indexed.col("text")).show();
		

		// Feature Transformers (StopWordsRemover)
		StopWordsRemover remover = new StopWordsRemover();
		String[] stopwords = remover.getStopWords();
		String[] newStopworks = new String[stopwords.length+2];
		newStopworks[0]="spam";
		newStopworks[1]="ham";
		for(int i=2;i<stopwords.length;i++){
			newStopworks[i]=stopwords[i];
		}	
		remover.setStopWords(newStopworks).setInputCol("text")
				.setOutputCol("filteredWords");
		Dataset<Row> filteredDF = remover.transform(indexed);
		filteredDF.select(filteredDF.col("label"), filteredDF.col("filteredWords")).show();

//		// Feature Extractors (HashingTF transformer)
		int numFeatures = 100;
		HashingTF hashingTF = new HashingTF().setInputCol("filteredWords")
				.setOutputCol("rawFeatures").setNumFeatures(numFeatures);
		Dataset<Row> featurizedData = hashingTF.transform(filteredDF);
		for (Row r : featurizedData.select("rawFeatures", "label").collectAsList()) {
			Vector features = r.getAs(0); ////Problematic line
		    Double label = r.getDouble(1);
			System.out.println(label + "," + features);
		}
//
		// Feature Extractors (IDF Estimator)
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		Dataset<Row> rescaledData = idfModel.transform(featurizedData);
		for (Row r : rescaledData.select("features", "label").collectAsList()) {
			Vector features = r.getAs(0);
			Double label = r.getDouble(1);
			System.out.println(label + "," + features);
		}
//
		org.apache.spark.ml.feature.ChiSqSelector selector = new org.apache.spark.ml.feature.ChiSqSelector();
		selector.setNumTopFeatures(3).setFeaturesCol("features")
				.setLabelCol("label").setOutputCol("selectedFeatures");
		Dataset<Row> result = selector.fit(rescaledData).transform(rescaledData);
		for (Row r : result.select("selectedFeatures", "label").collectAsList()) {
			Vector features = r.getAs(0);
			Double label = r.getDouble(1);
			System.out.println(label + "," + features);
		}
//
		Dataset<Row>[] splits = result.randomSplit(new double[] { 0.6, 0.4 });
		Dataset<Row> trainingData = splits[0];
		Dataset<Row> testData = splits[1];
		trainingData.show();
		//
		LogisticRegression logisticRegression = new LogisticRegression()
				.setMaxIter(10).setRegParam(0.01);

		//
		LogisticRegressionModel model = logisticRegression.fit(trainingData);
		// // Test Set Preparation
		Dataset<Row> predictions = model.transform(testData);
		predictions.show();
		long count=0;
		for (Row r : predictions.select("label", "filteredWords",
				"probability", "prediction").collectAsList()) {
			if(r.get(0).equals(r.get(3)))
				count++;
			System.out.println("(" + r.get(0) + ", " + r.get(1) + ") --> prob="
					+ r.get(2) + ", prediction=" + r.get(3));
		}
		System.out.println("precision: "+(double) (count*100)/predictions.count());		
	}
}
