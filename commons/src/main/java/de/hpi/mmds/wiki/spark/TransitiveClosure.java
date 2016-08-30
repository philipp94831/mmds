package de.hpi.mmds.wiki.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import static de.hpi.mmds.wiki.spark.SparkFunctions.identity;
import static de.hpi.mmds.wiki.spark.SparkFunctions.swap;

@Deprecated
public class TransitiveClosure {

	public static <T> JavaRDD<Tuple2<T, T>> compute(JavaRDD<Tuple2<T, T>> edges) {
		JavaPairRDD<T, T> tc = edges.mapToPair(t -> t).cache();
		JavaPairRDD<T, T> reversed = edges.mapToPair(swap()).cache();
		long oldCount;
		long nextCount = tc.count();
		do {
			oldCount = nextCount;
			JavaPairRDD<T, T> newEdges = tc.join(reversed).mapToPair(t -> new Tuple2<>(t._2._2, t._2._1));
			tc = tc.union(newEdges).distinct().cache();
			nextCount = tc.count();
		} while (nextCount != oldCount);
		return tc.map(identity());
	}

}
