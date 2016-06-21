package de.hpi.mmds.wiki;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Spark {

	private SparkConf conf = new SparkConf().setMaster("local");

	public Spark(String name) {
		conf.setAppName(name);
	}

	public static Spark newApp(String name) {
		return new Spark(name);
	}

	public JavaSparkContext context() {
		return new JavaSparkContext(conf);
	}

	public SparkConf getConf() {
		return conf;
	}

	public Spark set(String key, String value) {
		conf.set(key, value);
		return this;
	}

	public Spark setMaster(String master) {
		conf.setMaster(master);
		return this;
	}

	public Spark setWorkerMemory(String memory) {
		return set("spark.executor.memory", memory);
	}
}
