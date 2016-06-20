package de.hpi.mmds.wiki;

import de.hpi.mmds.wiki.categories.CategoryAnalyzer;
import de.hpi.mmds.wiki.cf.CollaborativeFiltering;
import de.hpi.mmds.wiki.lda.LDA;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class App {

	private static final String CF_DIR = null;
	private static final String CAT_DIR = null;
	private static final String GROUND_TRUTH = null;
	private static final String TRAINING_DIR = null;
	private static final String OUT_FILE = null;

	public static void main(String[] args) {
		try (JavaSparkContext jsc = Spark.getContext("MMDS Wiki"); FileSystem fs = FileSystem.getLocal()) {
			Edits training = new Edits(jsc, TRAINING_DIR);
			MultiRecommender recommender = new MultiRecommender().add(CollaborativeFiltering.load(jsc, CF_DIR, fs))
					.add(CategoryAnalyzer.load(jsc, CAT_DIR)).add(new LDA());
			File file = new File(OUT_FILE);
			File parentFile = file.getParentFile();
			if (parentFile != null) {
				parentFile.mkdirs();
			}
			if (file.exists()) {
				try {
					FileUtils.forceDelete(file);
				} catch (IOException e) {
					throw new RuntimeException("Could not delete output file " + file.getPath(), e);
				}
			}
			try (FileOutputStream out = new FileOutputStream(file)) {
				Evaluator eval = new Evaluator(recommender, training, GROUND_TRUTH, out);
				eval.evaluate(200, 100);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}