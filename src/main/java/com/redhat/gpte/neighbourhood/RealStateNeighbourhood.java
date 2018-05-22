package com.redhat.gpte.neighbourhood;

import java.io.Serializable;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

public class RealStateNeighbourhoodAnalysis implements Serializable {
	private static final long serialVersionUID = 1L;
	static SparkSession spark = SparkSession
			.builder().appName("RealEstateNeighbourHoodAnalysis")
			.master("local[*]")
			.config("spark.sql.warehouse.dir", "/tmp/")
			.getOrCreate();

	public static void main(String[] args) {
		RDD<String> data = spark.sparkContext().textFile("input/Sarotoga_Newyork_Homes.txt",1);
		JavaRDD<Vector> parsedData = data.toJavaRDD().map(s -> {
			String[] sarray = s.split(",");
			double[] values = new double[sarray.length];
			for (int i = 0; i < sarray.length; i++)
				values[i] = Double.parseDouble(sarray[i]);
			return Vectors.dense(values);
		});

		int numClusters = 4;
		int numIterations = 10;
		int runs = 2;
		KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations, runs , KMeans.K_MEANS_PARALLEL());
		
		double cost = clusters.computeCost(parsedData.rdd());
		System.out.println("Cost: " + cost);
		
		// Shows the result.
		Vector[] centers = clusters.clusterCenters();
		System.out.println("Cluster Centers: ");
		for (Vector center : centers) {
			System.out.println(center);
		}
		// Evaluate clustering by computing Within Set Sum of Squared Errors
		double WSSSE = clusters.computeCost(parsedData.rdd());
		System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
		
		List<Vector> houses = parsedData.collect();
		int prediction  = clusters.predict(houses.get(0));
		System.out.println("Prediction: "+prediction);
		System.out.println("Output to PMML"+clusters.toPMML());
		clusters.toPMML("NeighBourHood.pmml");
		spark.stop();
	}

}
