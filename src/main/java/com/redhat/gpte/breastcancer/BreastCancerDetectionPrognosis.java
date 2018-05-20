package com.redhat.gpte.breastcancer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BreastCancerDetectionPrognosis {
	static SparkSession spark = SparkSession.builder()
			.appName("BreastCancerDetectionPrognosis")
			.master("local[*]")
			.config("spark.sql.warehouse.dir", "/tmp/")
			.getOrCreate();

	public static void main(String[] args) {
		String path = "input/wdpc.data";
		JavaRDD<String> lines = spark.sparkContext().textFile(path, 3).toJavaRDD();
		JavaRDD<LabeledPoint> linesRDD = lines.map(lines1 -> {
			String[] tokens = lines1.split(",");
			double[] features = new double[30];
			for (int i = 2; i < features.length; i++) {
				features[i - 2] = Double.parseDouble(tokens[i]);
			}
			Vector v = new DenseVector(features);
			if (tokens[1].equals("N")) {
				return new LabeledPoint(1.0, v); // recurrent
			} else {
				return new LabeledPoint(0.0, v); // non-recurrent
			}
		});
		Dataset<Row> data = spark.createDataFrame(linesRDD, LabeledPoint.class);
		data.show();
		Dataset<Row>[] splits = data.randomSplit(new double[] { 0.6, 0.4 });
		Dataset<Row> trainingData = splits[0];
		Dataset<Row> testData = splits[1];
		// trainingData.show();
		LogisticRegression logisticRegression = new LogisticRegression().setMaxIter(100).setRegParam(0.01)
				.setElasticNetParam(0.4);

		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { logisticRegression });
		PipelineModel model = pipeline.fit(trainingData);

		// LogisticRegressionModel model = logisticRegression.fit(trainingData);
		Dataset<Row> predictions = model.transform(testData);
		predictions.show();
		long count = 0;
		for (Row r : predictions.select("features", "label", "prediction").collectAsList()) {
			System.out.println("(" + r.get(0) + ", " + r.get(1) + r.get(2) + ", prediction=" + r.get(2));
			count++;
		}
		System.out.println("precision: " + (double) (count * 100) / predictions.count());
	}
}
