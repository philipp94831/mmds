package de.hpi.mmds.wiki;

import de.hpi.mmds.wiki.spark.KeyFilter;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.io.Serializable;

public class Edits implements Serializable {

	private static final long serialVersionUID = 1668840974181477332L;
	private final JavaPairRDD<Integer, Integer> edits;

	public Edits(JavaSparkContext jsc, String dataDir) {
		edits = parseEdits(jsc, dataDir);
		edits.cache();
	}

	private static JavaPairRDD<Integer, Integer> parseEdits(JavaSparkContext jsc, String dataDir) {
		JavaRDD<String> data = jsc.textFile(dataDir);
		JavaPairRDD<Integer, Integer> edits = data.mapToPair(new PairFunction<String, Integer, Integer>() {

			private static final long serialVersionUID = -4781040078296911266L;

			@Override
			public Tuple2<Integer, Integer> call(String s) throws Exception {
				String[] sarray = s.split(",");
				return new Tuple2<>(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]));
			}
		});
		jsc.sc().log().info("Edit data loaded");
		return edits;
	}

	public JavaPairRDD<Integer, Iterable<Integer>> getAggregatedEdits() {
		return edits.groupByKey();
	}

	public JavaPairRDD<Integer, Integer> getAllEdits() {
		return edits;
	}

	public JavaRDD<Integer> getEdits(int user) {
		return edits.filter(new KeyFilter<>(user)).values();
	}

	public JavaRDD<Integer> getUsers() {
		return edits.keys();
	}

}
