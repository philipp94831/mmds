package de.hpi.mmds.wiki;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

import java.io.Serializable;

import static de.hpi.mmds.wiki.spark.SparkFunctions.identity;
import static de.hpi.mmds.wiki.spark.SparkFunctions.keyFilter;

public class Edits implements Serializable {

	private static final long serialVersionUID = 1668840974181477332L;
	private final JavaPairRDD<Integer, Iterable<Integer>> edits;

	public Edits(JavaSparkContext jsc, String dataDir) {
		edits = parseEdits(jsc, dataDir);
	}

	private static JavaPairRDD<Integer, Iterable<Integer>> parseEdits(JavaSparkContext jsc, String dataDir) {
		JavaRDD<String> data = jsc.textFile(dataDir);
		JavaPairRDD<Integer, Iterable<Integer>> edits = data.mapToPair(new PairFunction<String, Integer, Integer>() {

			private static final long serialVersionUID = -4781040078296911266L;

			@Override
			public Tuple2<Integer, Integer> call(String s) throws Exception {
				String[] sarray = s.split(",");
				return new Tuple2<>(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]));
			}
		}).groupByKey();
		jsc.sc().log().info("Edit data loaded");
		return edits;
	}

	public JavaPairRDD<Integer, Iterable<Integer>> getAggregatedEdits() {
		return edits;
	}

	public JavaPairRDD<Integer, Integer> getAllEdits() {
		return edits.flatMapValues(identity());
	}

	public JavaRDD<Integer> getEdits(int user) {
		return edits.filter(keyFilter(user)).flatMap(t -> t._2);
	}

	public JavaRDD<Integer> getUsers() {
		return edits.keys();
	}

	public Edits cache() {
		edits.cache();
		return this;
	}

	public Edits persist(StorageLevel level) {
		edits.persist(level);
		return this;
	}

}
