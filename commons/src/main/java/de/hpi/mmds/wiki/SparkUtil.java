package de.hpi.mmds.wiki;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkUtil {

	public static JavaSparkContext getContext(String name) {
		SparkConf conf = defaultConf(name);
		return new JavaSparkContext(conf);
	}

	private static SparkConf defaultConf(String name) {
		return new SparkConf().setAppName(name).setMaster("local");
	}

	public static JavaSparkContext getContext(String name, SparkConf newConf) {
		SparkConf conf = defaultConf(name);
		for (Tuple2<String, String> t : newConf.getAll()) {
			conf.set(t._1, t._2);
		}
		return new JavaSparkContext(conf);
	}
}
