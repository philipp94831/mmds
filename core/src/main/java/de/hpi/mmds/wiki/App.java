package de.hpi.mmds.wiki;

import org.apache.spark.api.java.JavaSparkContext;

import de.hpi.mmds.wiki.common.SparkUtil;

public class App {

	public static void main(String[] args) {
		try (JavaSparkContext jsc = SparkUtil.getContext()) {
			
		} catch(Exception e) {
			
		}
	}
}
