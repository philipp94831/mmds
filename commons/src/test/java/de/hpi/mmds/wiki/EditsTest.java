package de.hpi.mmds.wiki;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import scala.Tuple2;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EditsTest {

	private static Edits edits;
	private static JavaSparkContext jsc;

	@BeforeClass
	public static void setup() throws IOException {
		jsc = Spark.newApp(EditsTest.class.getName()).context();
		try (FileSystem fs = FileSystem.getLocal()) {
			edits = new Edits(jsc,
					Thread.currentThread().getContextClassLoader().getResource("sample_edits.txt").getPath(), fs);
		}
	}

	@AfterClass
	public static void tearDown() {
		jsc.close();
	}

	@Test
	public void testAggregatedEdits() {
		Map<Integer, Iterable<Integer>> e = edits.getAggregatedEdits().collectAsMap();
		assertEquals(3, e.size());
	}

	@Test
	public void testAllEdits() {
		List<Tuple2<Integer, Integer>> e = edits.getAllEdits().collect();
		assertEquals(7, e.size());
	}

	@Test
	public void testArticles() {
		List<Integer> articles = edits.getArticles().collect();
		assertEquals(5, articles.size());
		assertTrue(articles.contains(1));
		assertTrue(articles.contains(2));
		assertTrue(articles.contains(3));
		assertTrue(articles.contains(4));
		assertTrue(articles.contains(5));
	}

	@Test
	public void testEditsPerUser() {
		List<Integer> u1 = edits.getEdits(1).collect();
		assertEquals(2, u1.size());
		assertTrue(u1.contains(1));
		assertTrue(u1.contains(2));
	}

	@Test
	public void testUsers() {
		List<Integer> users = edits.getUsers().collect();
		assertEquals(3, users.size());
		assertTrue(users.contains(1));
		assertTrue(users.contains(2));
		assertTrue(users.contains(3));
	}
}
