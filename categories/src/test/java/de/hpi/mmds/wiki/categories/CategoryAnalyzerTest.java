package de.hpi.mmds.wiki.categories;

import de.hpi.mmds.wiki.Recommendation;
import de.hpi.mmds.wiki.Spark;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;

public class CategoryAnalyzerTest {
	private static final String SPARK_CONTEXT_NAME = "MMDS Wiki";
	private JavaSparkContext sc;
	private CategoryAnalyzer ca;
	private File treeFile;
	private File articleFile;

	@Before
	public void setUp() throws IOException {
		this.sc = Spark.getContext(SPARK_CONTEXT_NAME);

		this.treeFile = File.createTempFile("MMDSWiki_",".tmp");
		this.articleFile = File.createTempFile("MMDSWiki_",".tmp");

		(new FileWriter(this.treeFile)).write(
				"1,2\n" +
				"3,4"
		);

		(new FileWriter(this.articleFile)).write(
				"100,1"
		);

		this.ca = CategoryAnalyzer.build(this.sc, this.treeFile.getCanonicalPath(), this.articleFile.getCanonicalPath());
	}

	@Test
	@Ignore
	public void test() {
		List<Recommendation> recommended = this.ca.recommend(
			1, // userId
			sc.parallelize(Arrays.asList(500, 1000, 2000)), // articles a user has edited
			10 // number of recommendations to be returned
		);

		System.out.println(recommended);
	}

	@After
	public void tearDown()
	{
		treeFile.delete();
		articleFile.delete();
	}
}
