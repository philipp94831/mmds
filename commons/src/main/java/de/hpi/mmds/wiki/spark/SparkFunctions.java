package de.hpi.mmds.wiki.spark;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class SparkFunctions {

	public static <T, U> Function<Tuple2<T, U>, Boolean> keyFilter(T value) {
		return t -> t._1.equals(value);
	}

	public static <T> Function<T, T> identity() {
		return (v) -> v;
	}

}
