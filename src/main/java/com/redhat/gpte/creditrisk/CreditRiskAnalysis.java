package com.redhat.gpte.creditrisk;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreditRiskAnalysis {
	static SparkSession spark = SparkSession.builder()
			.appName("CreditRiskAnalysis")
			.master("local[*]")
			.config("spark.sql.warehouse.dir", "/tmp/")
			.getOrCreate();

	public static double parseDouble(String str) {
		return Double.parseDouble(str);
	}

	public static void main(String[] args) {
		String csvFile = "input/german_data.csv";
		Dataset<Row> df = spark.read().format("com.databricks.spark.csv").option("header", "false").load(csvFile);
		//df.show();
		JavaRDD<Credit> creditRDD = df.toJavaRDD().map(r -> new Credit(parseDouble(r.getString(0)), parseDouble(r.getString(1)) - 1,
				parseDouble(r.getString(2)), parseDouble(r.getString(3)), parseDouble(r.getString(4)),
				parseDouble(r.getString(5)), parseDouble(r.getString(6)) - 1, parseDouble(r.getString(7)) - 1,
				parseDouble(r.getString(8)), parseDouble(r.getString(9)) - 1, parseDouble(r.getString(10)) - 1,
				parseDouble(r.getString(11)) - 1, parseDouble(r.getString(12)) - 1,
				parseDouble(r.getString(13)), parseDouble(r.getString(14)) - 1,
				parseDouble(r.getString(15)) - 1, parseDouble(r.getString(16)) - 1,
				parseDouble(r.getString(17)) - 1, parseDouble(r.getString(18)) - 1,
				parseDouble(r.getString(19)) - 1, parseDouble(r.getString(20)) - 1));
		Dataset<Row> creditData = spark.sqlContext().createDataFrame(creditRDD, Credit.class);
		creditData.createOrReplaceTempView("credit");
		creditData.printSchema();
		
		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(new String[] { "balance", "duration", "history", "purpose", "amount", "savings",
						"employment", "instPercent", "sexMarried", "guarantors", "residenceDuration", "assets", "age",
						"concCredit", "apartment", "credits", "occupation", "dependents", "hasPhone", "foreign" })
				.setOutputCol("features");
		Dataset<Row> assembledFeatures = assembler.transform(creditData);
		assembledFeatures.show();
		
		StringIndexer creditabilityIndexer = new StringIndexer().setInputCol("creditability").setOutputCol("label");
		Dataset<Row> creditabilityIndexed = creditabilityIndexer.fit(assembledFeatures).transform(assembledFeatures);
		
		long splitSeed = 12345L;
		Dataset<Row>[] splits = creditabilityIndexed.randomSplit(new double[] { 0.7, 0.3 }, splitSeed);
		Dataset<Row> trainingData = splits[0];
		Dataset<Row> testData = splits[1];
		
		RandomForestClassifier classifier = new RandomForestClassifier()
				.setImpurity("gini")
				.setMaxDepth(3)
				.setNumTrees(20)
				.setFeatureSubsetStrategy("auto")
				.setSeed(splitSeed);

		RandomForestClassificationModel model = classifier.fit(trainingData);
		BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator().setLabelCol("label");
		
		Dataset<Row> predictions = model.transform(testData);
		model.toDebugString();
		
		double accuracy = evaluator.evaluate(predictions);
		System.out.println("accuracy after pipeline fitting: " + accuracy);
		RegressionMetrics rm = new RegressionMetrics(predictions);
		System.out.println("MSE: " + rm.meanSquaredError());

		System.out.println("MAE: " + rm.meanAbsoluteError());
		System.out.println("RMSE Squared: " + rm.rootMeanSquaredError());
		System.out.println("R Squared: " + rm.r2());
		System.out.println("Explained Variance: " + rm.explainedVariance() + "\n");
		df.show(false);
		creditabilityIndexed.show();
		//ArrayList<String> impurity = new ArrayList<>();
/*		// //impurity.add("entropy");
		// impurity.add("gini");
		// Iterable<String> iterable = impurity;
		// ParamMap[] paramGrid = new ParamGridBuilder()
		// .addGrid(classifier.maxBins(), new int[]{25, 31})
		// .addGrid(classifier.maxDepth(), new int[]{5, 10})
		// .addGrid(classifier.numTrees(), new int[]{20, 60})
		// .addGrid(classifier.im, iterable)
		// .build();
*/
	}

}