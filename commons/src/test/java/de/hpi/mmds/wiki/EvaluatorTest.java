package de.hpi.mmds.wiki;

import de.hpi.mmds.wiki.Evaluator.Result;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class EvaluatorTest {

	private static final double DOUBLE_TOLERANCE = 1e-3;
	private static final Recommender recommender = (userId, articles, howMany) -> Arrays
			.asList(new Recommendation(1.0, 1), new Recommendation(1.0, 2));
	private static Edits test;
	private static Edits training;
	private static JavaSparkContext jsc;
	private File file;
	private OutputStream out;

	@BeforeClass
	public static void setupClass() {
		jsc = Spark.getContext(EvaluatorTest.class.getName());
		test = new Edits(jsc, Thread.currentThread().getContextClassLoader().getResource("test_data.txt").getPath());
		training = new Edits(jsc,
				Thread.currentThread().getContextClassLoader().getResource("training_data.txt").getPath());
	}

	@AfterClass
	public static void tearDownClass() {
		jsc.close();
	}

	@Before
	public void setup() throws FileNotFoundException {
		file = new File("out.txt");
		out = new FileOutputStream(file);
	}

	@After
	public void tearDown() throws IOException {
		out.close();
		file.delete();
	}

	@Test
	public void test() throws FileNotFoundException {
		Evaluator eval = new Evaluator(recommender, test, training, out);
		Map<Integer, Result> results = eval.evaluate(3, -1);
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

	@Test
	public void testFromFile() throws FileNotFoundException {
		Evaluator eval = new Evaluator(recommender, training,
				Thread.currentThread().getContextClassLoader().getResource("ground_truth.csv").getPath(), out);
		Map<Integer, Result> results = eval.evaluate(3, -1);
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
