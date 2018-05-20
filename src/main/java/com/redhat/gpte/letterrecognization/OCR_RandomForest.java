package com.redhat.gpte.letterrecognization;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class OCR_RandomForest {
    static SparkSession spark = SparkSession
    		.builder()
    		.appName("JavaLDAExample")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "/tmp/").
            getOrCreate();

	public static void main(String[] args) {
		String input = "input/letterdata.data";
		Dataset<Row> df = spark.read().format("com.databricks.spark.csv").option("header", "true").load(input);
		df.show();
		final Map<String, Integer> alpha = new HashMap<String, Integer>();
		int count = 0;
		for(char i = 'A'; i <= 'Z'; i++){
			alpha.put(i + "", count++);		
			System.out.println(alpha);
		}
	
		JavaRDD<LabeledPoint> dataRDD = df.toJavaRDD().map(row -> {
			String letter = row.getString(0);
			double label = alpha.get(letter);
			double[] features= new double [16];
			for(int i = 1; i < row.size(); i++){
				features[i-1] = Double.parseDouble(row.getString(i));
			}
			Vector v = new DenseVector(features);				
			return new LabeledPoint(label, v);
		});
		
		//dataRDD.saveAsTextFile("Output/dataRDD");
		System.out.println(dataRDD.collect());
		
		//double[] weights = {0.6, 0.4};
		JavaRDD<LabeledPoint>[] splits = dataRDD.randomSplit(new double[] {0.7, 0.3}, 11L);
		JavaRDD<LabeledPoint> training = splits[0];
		JavaRDD<LabeledPoint> test = splits[1];		
	
		Integer numClasses = 26;
	    HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
	    Integer numTrees = 5; // Use more in practice.
	    String featureSubsetStrategy = "auto"; // Let the algorithm choose.
	    String impurity = "gini";
	    Integer maxDepth = 20;
	    Integer maxBins = 40;
	    Integer seed = 12345;
	    
		final RandomForestModel model = RandomForest.trainClassifier(training, numClasses,categoricalFeaturesInfo, numTrees, 
				                                                     featureSubsetStrategy, impurity, maxDepth, maxBins, seed);
	    // Compute raw scores on the test set.
	    JavaRDD<Tuple2<Object, Object>> predictionAndLabels = test.map(
	      p -> {
		  Double prediction = model.predict(p.features());
		  return new Tuple2<Object, Object>(prediction, p.label());
		}
	    );   

	    // Get evaluation metrics.
	    MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
	    System.out.println(metrics.confusionMatrix());
	    System.out.println(metrics.confusionMatrix());
	    double precision = metrics.precision(metrics.labels()[0]);
	    double recall = metrics.recall(metrics.labels()[0]);
	    double tp = 8.0;
	    double TP = metrics.truePositiveRate(tp);
	    double FP = metrics.falsePositiveRate(tp);
	    double WTP = metrics.weightedTruePositiveRate();
	    double WFP =  metrics.weightedFalsePositiveRate();
	    System.out.println("Precision = " + precision);
	    System.out.println("Recall = " + recall);
	    System.out.println("True Positive Rate = " + TP);
	    System.out.println("False Positive Rate = " + FP);
	    System.out.println("Weighted True Positive Rate = " + WTP);
	    System.out.println("Weighted False Positive Rate = " + WFP);
	}
}
