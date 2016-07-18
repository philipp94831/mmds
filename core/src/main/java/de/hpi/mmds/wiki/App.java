package de.hpi.mmds.wiki;

import de.hpi.mmds.wiki.cf.CollaborativeFiltering;
import de.hpi.mmds.wiki.lda.LDARecommender;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class App {

	private static final String NAME = "combined_mult0.5";
	public static final String DATA_DIR = "data/";
	private static final String CF_DIR = "filter/default_new";
	private static final String LDA_DIR = "ldamodel/advanced_model";
	private static final String TRAINING_DATA = DATA_DIR + "/training_new.txt";
	private static final String TEST_DATA = DATA_DIR + "/test_new.txt";
	private static final String DOCUMENTS = DATA_DIR + "2012advanced_articles.csv";
	private static final String GROUND_TRUTH = DATA_DIR + "ground_truth_new.csv";
	private static final String OUT_FILE = "log/eval_" + NAME + ".txt";

	public static void main(String[] args) {
		try (JavaSparkContext jsc = Spark.newApp("MMDS Wiki").setMaster("local[4]").setWorkerMemory("2g").context();
				FileSystem fs = FileSystem.getLocal()) {
			Edits training = new Edits(jsc, TRAINING_DATA, fs);
			MultiRecommender recommender = new MultiRecommender().add(CollaborativeFiltering.load(jsc, CF_DIR, fs))
					.add(0.5, LDARecommender.load(jsc, LDA_DIR, fs));
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
				Evaluator eval = new Evaluator(recommender, training, GROUND_TRUTH, out, fs);
				eval.evaluate(1000, 5000, 1L);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}