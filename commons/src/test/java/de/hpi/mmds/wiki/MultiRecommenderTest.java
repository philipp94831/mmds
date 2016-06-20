package de.hpi.mmds.wiki;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MultiRecommenderTest {

	public static final double DOUBLE_TOLERANCE = 0.001;
	private static JavaSparkContext jsc;

	@BeforeClass
	public static void setup() {
		jsc = Spark.getContext(MultiRecommenderTest.class.getName());
	}

	@AfterClass
	public static void tearDown() {
		jsc.close();
	}

	@Test
	public void test() {
		MultiRecommender recommender = new MultiRecommender();
		recommender.add(1.0,
				(userId, articles, howMany) -> Arrays.asList(new Recommendation(1.0, 1), new Recommendation(0.5, 2)));
		recommender.add(2.0,
				(userId, articles, howMany) -> Arrays.asList(new Recommendation(2.0, 1), new Recommendation(0.5, 3)));
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

	@Test
	public void testCascade() {
		MultiRecommender recommender = new MultiRecommender();
		recommender.add(1.0,
				(userId, articles, howMany) -> Arrays.asList(new Recommendation(1.0, 1), new Recommendation(0.5, 2)))
				.add(2.0, (userId, articles, howMany) -> Arrays
						.asList(new Recommendation(2.0, 1), new Recommendation(0.5, 3)));
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

	@Test
	public void testDefaultWeight() {
		MultiRecommender recommender = new MultiRecommender();
		recommender.add((userId, articles, howMany) -> Arrays
				.asList(new Recommendation(1.0, 1), new Recommendation(0.5, 2)))
				.add((userId, articles, howMany) -> Arrays
						.asList(new Recommendation(2.0, 1), new Recommendation(0.5, 3)));
		List<Recommendation> recommendations = recommender.recommend(-1, jsc.emptyRDD(), 10);
		Collections.sort(recommendations);
		assertEquals(3, recommendations.size());
		assertEquals(1, recommendations.get(0).getArticle());
		assertEquals(3.0 / 2.0, recommendations.get(0).getPrediction(), DOUBLE_TOLERANCE);
		assertEquals(2, recommendations.get(1).getArticle());
		assertEquals(0.5 / 2.0, recommendations.get(1).getPrediction(), DOUBLE_TOLERANCE);
		assertEquals(3, recommendations.get(2).getArticle());
		assertEquals(0.5 / 2.0, recommendations.get(2).getPrediction(), DOUBLE_TOLERANCE);
	}

}
