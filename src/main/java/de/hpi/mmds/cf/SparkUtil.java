package de.hpi.mmds.cf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkUtil {

	public static JavaSparkContext getContext() {
		SparkConf conf = new SparkConf().setAppName("Java Collaborative Filtering Example").setMaster("local[4]")
				.set("spark.executor.memory", "2g");
		return new JavaSparkContext(conf);
	}
}
