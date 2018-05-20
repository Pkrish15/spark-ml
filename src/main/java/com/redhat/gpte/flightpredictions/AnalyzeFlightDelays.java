package com.redhat.gpte.flightpredictions;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class AnalyzeFlightDelays {
	static SparkSession spark = SparkSession
			.builder()
			.appName("JavaLDAExample")
			.master("local[*]")
			.config("spark.sql.warehouse.dir", "/tmp/")
			.getOrCreate();
	
	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		String csvFile = "input/6403459_T_ONTIME.csv";
		//Dataset<Row> df = (new CsvParser()).withUseHeader(true).csvFile(spark.sqlContext(), csvFile);
		Dataset<Row> df = spark.read().format("com.databricks.spark.csv").option("header", "true").load(csvFile); 
		RDD<Tuple2<String, String>> distFile = spark.sparkContext().wholeTextFiles("input/test/*.txt", 2);
		@SuppressWarnings("unused")
		JavaRDD<Tuple2<String, String>> distFile2 = distFile.toJavaRDD();

        @SuppressWarnings("unused")
		JavaRDD<Row> rowRDD = df.toJavaRDD();
		
		Dataset<Row> newDF = df.select(df.col("monthDay"), df.col("weekDay"),
				df.col("CRSDepTime"), df.col("CRSArrTime"), df.col("Carrier"),
				df.col("CRSElapsedTime"), df.col("Origin"), df.col("Dest"));
		newDF.show(5);
		
		JavaRDD<Flight> flightsRDD = newDF.toJavaRDD().filter(v1 -> !v1.getString(0).isEmpty()).map(r -> {
					double label;
					double delay = Double.parseDouble(r.getString(0));
					if (delay > 60)
						label = 1.0;
					else
						label = 0.0;
					double monthday = Double.parseDouble(r.getString(1)) - 1;
					double weekday = Double.parseDouble(r.getString(2)) - 1;
					double crsdeptime = Double.parseDouble(r.getString(3));
					double crsarrtime = Double.parseDouble(r.getString(4));
					String carrier = r.getString(5);
					double crselapsedtime1 = Double.parseDouble(r.getString(6));
					String origin = r.getString(7);
					String dest = r.getString(8);
					double ArrDelayMinutes=Double.parseDouble(r.getString(9));
					double distance=Double.parseDouble(r.getString(10));
					
					Flight flight = new Flight(label,monthday,weekday,crsdeptime,ArrDelayMinutes,distance,crsarrtime, carrier,
							crselapsedtime1, origin, dest); 
					return flight;
				});

		Dataset<Row> flightDelayData = spark.sqlContext().createDataFrame(flightsRDD,Flight.class);
		flightDelayData.printSchema();
		//flightDelayData.show(5);

		StringIndexer carrierIndexer = new StringIndexer().setInputCol("carrier").setOutputCol("carrierIndex");
		Dataset<Row> carrierIndexed = carrierIndexer.fit(flightDelayData).transform(flightDelayData);

		StringIndexer originIndexer = new StringIndexer().setInputCol("origin").setOutputCol("originIndex");
		Dataset<Row> originIndexed = originIndexer.fit(carrierIndexed).transform(carrierIndexed);

		StringIndexer destIndexer = new StringIndexer().setInputCol("dest").setOutputCol("destIndex");
		Dataset<Row> destIndexed = destIndexer.fit(originIndexed).transform(originIndexed);
		destIndexed.show(5);
		
		VectorAssembler assembler = new VectorAssembler().setInputCols(
				new String[] { "monthDay", "weekDay", "crsdeptime",
						"crsarrtime", "carrierIndex", "crselapsedtime",
						"originIndex", "destIndex" }).setOutputCol(
				"assembeledVector");
		Dataset<Row> assembledFeatures = assembler.transform(destIndexed);
	
		JavaRDD<Row> rescaledRDD = assembledFeatures.select("label","assembeledVector").toJavaRDD();
		System.out.println(rescaledRDD.take(1));
		JavaRDD<LabeledPoint> mlData = rescaledRDD.map(row -> {
			double label = row.getDouble(0);
			Vector v = row.getAs(1);
			return new LabeledPoint(label, v);
		});
		System.out.println(mlData.take(2));

		JavaRDD<LabeledPoint> splitedData0 = mlData.filter(r -> r.label() == 0).randomSplit(new double[] { 0.85, 0.15 })[1];

		JavaRDD<LabeledPoint> splitedData1 = mlData.filter(r -> r.label() == 1);
	
		JavaRDD<LabeledPoint> splitedData2 = splitedData1.union(splitedData0);
		System.out.println(splitedData2.take(1));
		
		Dataset<Row> data = spark.sqlContext().createDataFrame(splitedData2, LabeledPoint.class);
		data.show(100);
	
		VectorIndexerModel featureIndexer = new VectorIndexer()
				  .setInputCol("features")
				  .setOutputCol("indexedFeatures")
				  .setMaxCategories(4)
				  .fit(data);
		// features with > 4 distinct values are treated as continuous.
		StringIndexerModel labelIndexer = new StringIndexer()
				  .setInputCol("label")
				  .setOutputCol("indexedLabel")
				  .fit(data);
		Dataset<Row>[] splits = data.randomSplit(new double[]{0.7, 0.3});
		Dataset<Row> trainingData = splits[0];
		Dataset<Row> testData = splits[1];
		
		// Train a DecisionTree model.
		DecisionTreeClassifier dt = new DecisionTreeClassifier()
		  .setLabelCol("indexedLabel")
		  .setFeaturesCol("indexedFeatures");


		// Convert indexed labels back to original labels.
		IndexToString labelConverter = new IndexToString()
		  .setInputCol("prediction")
		  .setOutputCol("predictedLabel")
		  .setLabels(labelIndexer.labels());

		// Chain indexers and tree in a Pipeline.
		Pipeline pipeline = new Pipeline()
		  .setStages(new PipelineStage[]{labelIndexer, featureIndexer, dt, labelConverter});

		// Train model. This also runs the indexers.
		PipelineModel model = pipeline.fit(trainingData);

		// Make predictions.
		Dataset<Row> predictions = model.transform(testData);

		// Select example rows to display.
		predictions.select("predictedLabel", "label", "features").show(5);

		// Select (prediction, true label) and compute test error.
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
		  .setLabelCol("indexedLabel")
		  .setPredictionCol("prediction")
		  .setMetricName("accuracy");
		double accuracy = evaluator.evaluate(predictions);
		System.out.println("Accuracy: "+accuracy);
		System.out.println("Test Error = " + (1.0 - accuracy));

		DecisionTreeClassificationModel treeModel =
		  (DecisionTreeClassificationModel) (model.stages()[2]);
		System.out.println("Learned classification tree model:\n" + treeModel.toDebugString());
		spark.stop();
	}
}
