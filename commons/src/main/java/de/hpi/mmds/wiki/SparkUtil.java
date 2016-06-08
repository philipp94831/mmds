package de.hpi.mmds.wiki;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkUtil {

	public static JavaSparkContext getContext() {
		SparkConf conf = defaultConf();
		return new JavaSparkContext(conf);
	}

	private static SparkConf defaultConf() {
		return new SparkConf().setAppName("MMDS Wiki").setMaster("local");
	}

	public static JavaSparkContext getContext(SparkConf newConf) {
		SparkConf conf = defaultConf();
		for (Tuple2<String, String> t : newConf.getAll()) {
			conf.set(t._1, t._2);
		}
		return new JavaSparkContext(conf);
	}
}
