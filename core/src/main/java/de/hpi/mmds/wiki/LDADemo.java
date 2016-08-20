package de.hpi.mmds.wiki;

import de.hpi.mmds.wiki.lda.LDARecommender;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class LDADemo {

	private static final String NAME = "advanced_model";
	private static final String MODEL = "ldamodel/" + NAME;
	public static final String DATA_DIR = "data/";
	private static final String DOCUMENTS = DATA_DIR + "2012advanced_articles.csv";
	private static final String TRAINING_DATA = DATA_DIR + "training_new.txt";
	private static final String TEST_DATA = DATA_DIR + "edits/test*.txt";
	private static final String GROUND_TRUTH = DATA_DIR + "ground_truth_new.csv";
	private static final String OUT_FILE = "log/eval_lda_" + NAME + "_small.txt";
	private static final int NUM_TOPICS = 400;
	private static final int ITERATIONS = 10;
	public static final String HDFS_HOST = "";

	public static void main(String[] args) {
		try (FileSystem fs = FileSystem.getLocal()) {
			if (!fs.exists(MODEL)) {
				try (JavaSparkContext jsc = Spark.newApp("MMDS Wiki").setMaster("local[4]").setWorkerMemory("1g")
						.setResultSize("6g").context()) {
					LDARecommender.train(jsc, DOCUMENTS, NUM_TOPICS, ITERATIONS, fs).save(MODEL, fs);
				}
			}
			try (JavaSparkContext jsc = Spark.newApp("MMDS Wiki").setMaster("local[4]").setWorkerMemory("2g")
					.context()) {
				Recommender r = LDARecommender.load(jsc, MODEL, fs);
				Edits edits = new Edits(jsc, TRAINING_DATA, fs);
				File file = new File(OUT_FILE);
				file.getParentFile().mkdirs();
				if (file.exists()) {
					FileUtils.forceDelete(file);
				}
				try (OutputStream out = new FileOutputStream(file)) {
					Evaluator eval = new Evaluator(r, edits, GROUND_TRUTH, out, fs);
					eval.evaluate(1000, 10, 1L);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
