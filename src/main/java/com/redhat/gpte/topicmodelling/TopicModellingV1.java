package com.redhat.gpte.topicmodelling;

import java.io.FileNotFoundException;
import java.io.Serializable;

import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.feature.ChiSqSelector;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class TopicModellingV1 implements Serializable {
	private static final long serialVersionUID = 1L;
	static SparkSession spark = SparkSession
    		.builder()
    		.appName("JavaLDAExample")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "/tmp/").
            getOrCreate();

	public static void main(String[] args) throws FileNotFoundException {
		Dataset<Row> df = spark.read().text("input/test/*.txt");
		//df.show();

		// Feature Transformers (RegexTokenizer)
		RegexTokenizer regexTokenizer1 = new RegexTokenizer().setInputCol("value").setOutputCol("labelText").setPattern("\\t.*$");

		Dataset<Row> labelTextDataFrame = regexTokenizer1.transform(df);
		RegexTokenizer regexTokenizer2 = new RegexTokenizer().setInputCol("value").setOutputCol("text").setPattern("\\W");
		Dataset<Row> labelFeatureDataFrame = regexTokenizer2.transform(labelTextDataFrame);
/*		for (Row r : labelFeatureDataFrame.select("text", "labelText").collect()) {
			//System.out.println(r.getAs(1) + ": " + r.getAs(0));
		}*/

		Dataset<Row> newDF = labelFeatureDataFrame
				.withColumn("labelTextTemp", labelFeatureDataFrame.col("labelText").cast(DataTypes.StringType))
				.drop(labelFeatureDataFrame.col("labelText")).withColumnRenamed("labelTextTemp", "labelText");

		// Feature Transformer (StringIndexer)
		StringIndexer indexer = new StringIndexer().setInputCol("labelText").setOutputCol("label");
		Dataset<Row> indexed = indexer.fit(newDF).transform(newDF);
		//indexed.select(indexed.col("labelText"), indexed.col("label"), indexed.col("text")).show();

		// Feature Transformers (StopWordsRemover)
		StopWordsRemover remover = new StopWordsRemover();
		String[] stopwords = remover.getStopWords();
/*		String[] newStopwords = new String[stopwords.length + 2];
		newStopwords[0] = "spam";
		newStopwords[1] = "ham";
		for (int i = 2; i < stopwords.length; i++) {
			newStopwords[i] = stopwords[i];
		}*/
		remover.setStopWords(stopwords).setInputCol("text").setOutputCol("filteredWords");
		Dataset<Row> filteredDF = remover.transform(indexed);
		//filteredDF.show();
		//filteredDF.select(filteredDF.col("label"), filteredDF.col("filteredWords")).show();

		// // Feature Extractors (HashingTF transformer)
		int numFeatures = 100;
		HashingTF hashingTF = new HashingTF().setInputCol("filteredWords").setOutputCol("rawFeatures").setNumFeatures(numFeatures);
		Dataset<Row> featurizedData = hashingTF.transform(filteredDF);
/*		for (Row r : featurizedData.select("rawFeatures", "label").collect()) {
			Vector features = r.getAs(0);
			Double label = r.getDouble(1);
			//System.out.println(label + "," + features);
		}*/
		//
		// Feature Extractors (IDF Estimator)
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		Dataset<Row> rescaledData = idfModel.transform(featurizedData);
		
/*		for (Row r : rescaledData.select("features", "label").collect()) {
			Vector features = r.getAs(0);
			Double label = r.getDouble(1);
			//System.out.println(label + "," + features);
		}*/
		//
		ChiSqSelector selector = new org.apache.spark.ml.feature.ChiSqSelector();
		selector.setNumTopFeatures(10).setFeaturesCol("features").setLabelCol("label").setOutputCol("selectedFeatures");
		
		Dataset<Row> result = selector.fit(rescaledData).transform(rescaledData);
/*		result.write().parquet("Output/feature");;
		result.select("features").show();
		for (Row r : result.select("selectedFeatures", "label").collect()) {
			Vector features = r.getAs(0);
			Double label = r.getDouble(1);
			//System.out.println(label + "," + features);
		}
		//
		DataFrame[] splits = result.randomSplit(new double[] { 0.7, 0.3 });
		DataFrame trainingData = splits[0];
		DataFrame testData = splits[1];
		//trainingData.show();
		//
*/		
		  long value = 5;
		// Trains a LDA model
		  LDA lda = new LDA().setK(10).setMaxIter(10).setSeed(value );
		  LDAModel model = lda.fit(result);

		  //System.out.println(model.getOldTopicConcentration());
		  //System.out.println(model.logLikelihood(result));
		  //System.out.println(model.logPerplexity(result));

		  // Shows the result
		 System.out.println(model.vocabSize());
		 Dataset<Row> topics = model.describeTopics(5);
		 org.apache.spark.ml.linalg.Matrix metric = model.topicsMatrix();
		 System.out.println(metric);
		 topics.show(false);
	}
}
