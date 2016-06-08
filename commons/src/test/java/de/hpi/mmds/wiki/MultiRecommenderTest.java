package de.hpi.mmds.wiki;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MultiRecommenderTest {

	public static final double DOUBLE_TOLERANCE = 0.001;
	private static Edits edits;
	private static JavaSparkContext jsc;

	@BeforeClass
	public static void setup() {
		jsc = SparkUtil.getContext();
		edits = new Edits(jsc, Thread.currentThread().getContextClassLoader().getResource("test_data.txt").getPath());
	}

	@AfterClass
	public static void tearDown() {
		jsc.close();
	}

	@Test
	public void test() {
		MultiRecommender recommender = new MultiRecommender();
		recommender.add(1.0, new Recommender() {

			@Override
			public List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany) {
				return Arrays.asList(new Recommendation(1.0, 1), new Recommendation(0.5, 2));
			}
		});
		recommender.add(2.0, new Recommender() {

			@Override
			public List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany) {
				return Arrays.asList(new Recommendation(2.0, 1), new Recommendation(0.5, 3));
			}
		});
		List<Recommendation> recommendations = recommender.recommend(-1, jsc.emptyRDD(), 10);
		Collections.sort(recommendations);
		assertEquals(3, recommendations.size());
		assertEquals(1, recommendations.get(0).getArticle());
		assertEquals(5.0 / 3.0, recommendations.get(0).getPrediction(), DOUBLE_TOLERANCE);
		assertEquals(3, recommendations.get(1).getArticle());
		assertEquals(1.0 / 3.0, recommendations.get(1).getPrediction(), DOUBLE_TOLERANCE);
		assertEquals(2, recommendations.get(2).getArticle());
		assertEquals(0.5 / 3.0, recommendations.get(2).getPrediction(), DOUBLE_TOLERANCE);
	}

}
