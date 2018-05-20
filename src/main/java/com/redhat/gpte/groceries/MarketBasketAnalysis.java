package com.redhat.gpte.groceries;

//Step-1: Load required packages and APIs
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple4;

/*
 * @author Praveen Kumar KS (Prakrish@redhat.com)
 * 
 */

public class MarketBasketAnalysis {
	static Integer frequency = 0;

	// This method convert the raw transaction and returns them as a list of
	// String
	static List<String> toList(String transaction) {
		String[] items = transaction.trim().split(",");
		List<String> list = new ArrayList<String>();
		for (String item : items) {
			list.add(item);
		}
		return list;
	}

	// This method removes the null and removes infrequent-1 itemsets
	static List<String> removeOneItem(List<String> list, int i) {
		if ((list == null) || (list.isEmpty())) {
			return list;
		}
		if ((i < 0) || (i > (list.size() - 1))) {
			return list;
		}
		List<String> cloned = new ArrayList<String>(list);
		cloned.remove(i);
		return cloned;
	}

	public static void main(String[] args) throws Exception {
		// Step 2: create a Spark session
		SparkSession spark = SparkSession.builder().appName("MarketBasketAnalysis").master("local[*]")
				.config("spark.sql.warehouse.dir", "/tmp/").getOrCreate();

		// Step 3: read all transactions from local disk or HDFS and create the
		// transaction RDD
		String transactionsFileName = "input/groceries.csv";
		RDD<String> transactions = spark.sparkContext().textFile(transactionsFileName, 1);
		transactions.saveAsTextFile("output/transactions");

		// Step 4: generate frequent patterns => map - phase 1
		JavaPairRDD<List<String>, Integer> patterns = transactions.toJavaRDD()
				.flatMapToPair(transaction -> {
					List<String> list = toList(transaction);
					List<List<String>> combinations = Combination.findSortedCombinations(list);
					List<Tuple2<List<String>, Integer>> result = new ArrayList<Tuple2<List<String>, Integer>>();
					for (List<String> combList : combinations) {
						if (combList.size() > 0) {
							result.add(new Tuple2<List<String>, Integer>(combList, 1));
						}
					}
					return result.iterator();
				});
		patterns.saveAsTextFile("output/1itemsets");

		// Step 5: combine/reduce frequent patterns => reduce phase-1
		JavaPairRDD<List<String>, Integer> combined = patterns.reduceByKey((i1, i2) -> {
			int support = 0;
			if (i1 + i2 >= 2) {
				support = i1 + i2;
			}
			// if(support >= 2)
			return support;
		});
		combined.saveAsTextFile("output/frequent_patterns");

		// Step 6: generate all the sub-patterns => map phase-2
		JavaPairRDD<List<String>, Tuple2<List<String>, Integer>> candidate_patterns = combined.flatMapToPair(
				pattern -> {
List<Tuple2<List<String>, Tuple2<List<String>, Integer>>> result = new ArrayList<Tuple2<List<String>, Tuple2<List<String>, Integer>>>();
List<String> list = pattern._1;
frequency = pattern._2;
result.add(new Tuple2(list, new Tuple2(null, frequency)));
if (list.size() == 1) {
				return result.iterator();
}

// pattern has more than one item
// result.add(new Tuple2(list, new Tuple2(null,size)));
for (int i = 0; i < list.size(); i++) {
				List<String> sublist = removeOneItem(list, i);
				result.add(new Tuple2<List<String>, Tuple2<List<String>, Integer>>(sublist,
						new Tuple2(list, frequency)));
}
return result.iterator();
});
		candidate_patterns.saveAsTextFile("output/sub_patterns");

		// Step 7: combine all the sub-patterns to generate simple rules
		JavaPairRDD<List<String>, Iterable<Tuple2<List<String>, Integer>>> rules = candidate_patterns.groupByKey();
		// rules.saveAsTextFile("output/combined_subpatterns");

		// Step 8: generate all the association rules => reduce phase-2. We also
		// calculate the support, confidence and lift and pick the selected
		// rules
		JavaRDD<List<Tuple4<List<String>, List<String>, Double, Double>>> assocRules = rules.map(
				new Function<Tuple2<List<String>, Iterable<Tuple2<List<String>, Integer>>>, List<Tuple4<List<String>, List<String>, Double, Double>>>() {
					@Override
					public List<Tuple4<List<String>, List<String>, Double, Double>> call(
							Tuple2<List<String>, Iterable<Tuple2<List<String>, Integer>>> in) throws Exception {

						List<Tuple4<List<String>, List<String>, Double, Double>> result = new ArrayList<Tuple4<List<String>, List<String>, Double, Double>>();
						List<String> fromList = in._1;
						Iterable<Tuple2<List<String>, Integer>> to = in._2;
						List<Tuple2<List<String>, Integer>> toList = new ArrayList<Tuple2<List<String>, Integer>>();
						Tuple2<List<String>, Integer> fromCount = null;
						for (Tuple2<List<String>, Integer> t2 : to) {
							// find the "count" object
							if (t2._1 == null) {
								fromCount = t2;
							} else {
								toList.add(t2);
							}
						}

						// Although no output generated, but since Spark does
						// not like null objects, we will fake a null object
						if (toList.isEmpty()) {
							return result;
						}

						for (Tuple2<List<String>, Integer> t2 : toList) {
							double confidence = (double) t2._2 / (double) fromCount._2;
							double lift = confidence / (double) t2._2;
							double support = (double) fromCount._2;
							List<String> t2List = new ArrayList<String>(t2._1);
							t2List.removeAll(fromList);
							if (support >= 2.0 && fromList != null && t2List != null) {
								result.add(new Tuple4(fromList, t2List, support, confidence));
								System.out.println(
										fromList + "=>" + t2List + "," + support + "," + confidence + "," + lift);
							}
						}
						return result;
					}
				});

		assocRules.saveAsTextFile("output/association_rules_with_conf_lift");
		System.exit(0);
	}
}