package de.hpi.mmds.wiki;

import de.hpi.mmds.wiki.categories.CategoryAnalyzer;
import de.hpi.mmds.wiki.cf.CollaborativeFiltering;
import de.hpi.mmds.wiki.lda.LDA;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;

public class App {

	private static final String CF_DIR = null;
	private static final String CAT_DIR = null;
	private static final String TEST_DIR = null;
	private static final String TRAINING_DIR = null;
	private static final String OUT_FILE = null;

	public static void main(String[] args) {
		try (JavaSparkContext jsc = Spark.getContext("MMDS Wiki"); HDFS fs = HDFS.getLocal()) {
			Edits test = new Edits(jsc, TEST_DIR);
			Edits training = new Edits(jsc, TRAINING_DIR);
			MultiRecommender recommender = new MultiRecommender().add(CollaborativeFiltering.load(jsc, CF_DIR, fs))
					.add(CategoryAnalyzer.load(jsc, CAT_DIR)).add(new LDA());
			Evaluator eval = new Evaluator(recommender, test, training, new File(OUT_FILE));
			eval.evaluate(200, 100);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}