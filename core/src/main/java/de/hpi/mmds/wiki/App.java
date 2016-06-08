package de.hpi.mmds.wiki;

import de.hpi.mmds.wiki.categories.CategoryAnalyzer;
import de.hpi.mmds.wiki.cf.CollaborativeFiltering;
import de.hpi.mmds.wiki.lda.LDA;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;

public class App {

	private static final String CF_FILTER_DIR = null;
	private static final String TEST_DIR = null;
	private static final String TRAINING_DIR = null;
	private static final String OUT_FILE = null;

	public static void main(String[] args) {
		try (JavaSparkContext jsc = SparkUtil.getContext()) {
			Edits test = new Edits(jsc, TEST_DIR);
			Edits training = new Edits(jsc, TRAINING_DIR);
			MultiRecommender recommender = new MultiRecommender();
			recommender.add(1, new CollaborativeFiltering(jsc, CF_FILTER_DIR));
			recommender.add(1, new CategoryAnalyzer());
			recommender.add(1, new LDA());
			Evaluator eval = new Evaluator(recommender, test, training, new File(OUT_FILE));
			eval.evaluate(200);
		}
	}
}
