package de.hpi.mmds.wiki;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

import java.io.Serializable;

import static de.hpi.mmds.wiki.spark.SparkFunctions.identity;
import static de.hpi.mmds.wiki.spark.SparkFunctions.keyFilter;

public class Edits implements Serializable {

	private static final long serialVersionUID = 1668840974181477332L;
	private final JavaPairRDD<Integer, Iterable<Integer>> edits;

	public Edits(JavaSparkContext jsc, String dataDir, FileSystem fs) {
		edits = jsc.textFile(fs.makeQualified(dataDir).toString()).mapToPair(Edits::parseEdits).groupByKey();
	}

	private static Tuple2<Integer, Integer> parseEdits(String s) {
		String[] sarray = s.split(",");
		return new Tuple2<>(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]));
	}

	public Edits cache() {
		edits.cache();
		return this;
	}

	public JavaPairRDD<Integer, Iterable<Integer>> getAggregatedEdits() {
		return edits;
	}

	public JavaPairRDD<Integer, Integer> getAllEdits() {
		return edits.flatMapValues(identity());
	}

	public JavaRDD<Integer> getArticles() {
		return edits.values().flatMap(i -> i).distinct();
	}

	public JavaRDD<Integer> getEdits(int user) {
		return edits.filter(keyFilter(user)).flatMap(t -> t._2);
	}

	public JavaRDD<Integer> getUsers() {
		return edits.keys().distinct();
	}

	public Edits persist(StorageLevel level) {
		edits.persist(level);
		return this;
	}

}
