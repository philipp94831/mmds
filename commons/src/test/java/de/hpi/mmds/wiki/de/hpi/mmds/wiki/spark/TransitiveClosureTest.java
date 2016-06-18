package de.hpi.mmds.wiki.de.hpi.mmds.wiki.spark;

import de.hpi.mmds.wiki.Spark;
import de.hpi.mmds.wiki.spark.TransitiveClosure;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TransitiveClosureTest {

	private static JavaSparkContext jsc;

	@BeforeClass
	public static void setupClass() {
		jsc = Spark.getContext(TransitiveClosureTest.class.getName());
	}

	@AfterClass
	public static void tearDownClass() {
		jsc.close();
	}

	@Test
	public void test() {
		JavaRDD<Tuple2<Integer, Integer>> edges = jsc
				.parallelize(Arrays.asList(new Tuple2<>(3, 2), new Tuple2<>(2, 1), new Tuple2<>(4, 1)));
		List<Tuple2<Integer, Integer>> tc = TransitiveClosure.compute(edges).collect();
		assertEquals(4, tc.size());
		assertTrue(tc.contains(new Tuple2<>(3, 2)));
		assertTrue(tc.contains(new Tuple2<>(3, 1)));
		assertTrue(tc.contains(new Tuple2<>(2, 1)));
		assertTrue(tc.contains(new Tuple2<>(4, 1)));
	}

}
