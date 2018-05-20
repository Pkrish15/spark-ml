package com.redhat.gpte.mllib.pipeline;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PipelineML implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5806708585193284934L;
	static SparkSession spark = SparkSession.builder().appName("JavaLDAExample").master("local[*]")
			.config("spark.sql.warehouse.dir", "/tmp/").getOrCreate();

	public static void main(String[] args) {
		// Prepare training documents, which are labeled.
		Dataset<Row> smsspamdataset = spark.createDataFrame(Arrays.asList(
		  new SMSSpamHamLabelDocument(0.0, "What you doing how are you"),
		  new SMSSpamHamLabelDocument(0.0, "Ok lar Joking wif u oni"),
		  new SMSSpamHamLabelDocument(1.0, "FreeMsg Txt CALL to No 86888 & claim your reward of 3 hours talk time to use from your phone now ubscribe6GBP mnth inc 3hrs 16 stop txtStop"),
		  new SMSSpamHamLabelDocument(0.0, "dun say so early hor U c already then say"),
		  new SMSSpamHamLabelDocument(0.0, "MY NO IN LUTON 0125698789 RING ME IF UR AROUND H"),
		  new SMSSpamHamLabelDocument(1.0, "Sunshine Quiz Win a super Sony DVD recorder if you canname the capital of Australia Text MQUIZ to 82277 B")
		), SMSSpamHamLabelDocument.class);
		
		smsspamdataset.show();

		Dataset<Row>[] splits = smsspamdataset.randomSplit(new double[] { 0.6, 0.4 });
		Dataset<Row> trainingData = splits[0];
		Dataset<Row> testData = splits[1];
		
		Tokenizer tokenizer = new Tokenizer()
		  .setInputCol("wordText")
		  .setOutputCol("words");
		HashingTF hashingTF = new HashingTF()
		  .setNumFeatures(100)
		  .setInputCol(tokenizer.getOutputCol())
		  .setOutputCol("features");
		LogisticRegression logisticRegression = new LogisticRegression()
		  .setMaxIter(10)
		  .setRegParam(0.01);
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {tokenizer, hashingTF, logisticRegression});
		// Fit the pipeline to training documents.
		PipelineModel model = pipeline.fit(trainingData);
		Dataset<Row> predictions = model.transform(testData);
		for (Row r: predictions.select("label", "wordText", "prediction").collectAsList()) {
			  System.out.println("(" + r.get(0) + ", " + r.get(1) + ") --> prediction=" + r.get(2));
		}
	}
}
