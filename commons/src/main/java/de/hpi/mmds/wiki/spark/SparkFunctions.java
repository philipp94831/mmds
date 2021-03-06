package de.hpi.mmds.wiki.spark;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkFunctions {

	public static <T> Function<T, T> identity() {
		return v -> v;
	}

	public static <T, U> Function<Tuple2<T, U>, Boolean> keyFilter(T value) {
		return t -> t._1.equals(value);
	}

	public static <T, U> PairFunction<Tuple2<T, U>, U, T> swap() {
		return t -> new Tuple2<>(t._2, t._1);
	}

}
