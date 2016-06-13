package de.hpi.mmds.wiki;

import de.hpi.mmds.wiki.Evaluator.Result;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class EvaluatorTest {

	public static final double DOUBLE_TOLERANCE = 0.001;
	private static final Recommender recommender = (userId, articles, howMany) -> Arrays
			.asList(new Recommendation(1.0, 10), new Recommendation(1.0, 11));
	private static Edits test;
	private static Edits training;
	private static JavaSparkContext jsc;
	private File out;

	@BeforeClass
	public static void setupClass() {
		jsc = SparkUtil.getContext();
		test = new Edits(jsc, Thread.currentThread().getContextClassLoader().getResource("test_data.txt").getPath());
		training = new Edits(jsc,
				Thread.currentThread().getContextClassLoader().getResource("training_data.txt").getPath());
	}

	@AfterClass
	public static void tearDownClass() {
		jsc.close();
	}

	@Before
	public void setup() {
		out = new File("out.txt");
	}

	@After
	public void tearDown() {
		out.delete();
	}

	@Test
	public void test() {
		Evaluator eval = new Evaluator(recommender, test, training, out);
		Map<Integer, Result> results = eval.evaluate(3);
		assertEquals(3, results.size());
		assertEquals(1.0, results.get(1).precision(), DOUBLE_TOLERANCE);
		assertEquals(1.0, results.get(1).meanAveragePrecision(), DOUBLE_TOLERANCE);
		assertEquals(1.0, results.get(1).recall(), DOUBLE_TOLERANCE);
		assertEquals(0.5, results.get(2).precision(), DOUBLE_TOLERANCE);
		assertEquals(1.0, results.get(2).meanAveragePrecision(), DOUBLE_TOLERANCE);
		assertEquals(1.0 / 3, results.get(2).recall(), DOUBLE_TOLERANCE);
		assertEquals(0.0, results.get(3).precision(), DOUBLE_TOLERANCE);
		assertEquals(0.0, results.get(3).meanAveragePrecision(), DOUBLE_TOLERANCE);
		assertEquals(0.0, results.get(3).recall(), DOUBLE_TOLERANCE);
	}

}
