package de.hpi.mmds.wiki;

import de.hpi.mmds.wiki.lda.LDA_Recommender;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

public class LDADemo {

	private static final String NAME = "2012";
	private static final String MODEL = "ldamodel/" + NAME;
	public static final String DATA_DIR = "/data/mmds16/wiki/";
	private static final String DOCUMENTS = DATA_DIR + "2012articles.csv";
	private static final String TRAINING_DATA = DATA_DIR + "training.txt";
	private static final String GROUND_TRUTH = DATA_DIR + "ground_truth.csv";
	private static final String OUT_FILE = "log/eval_" + NAME + ".txt";
	private static final int NUM_TOPICS = 200;
	private static final int ITERATIONS = 10;
	public static final String HDFS_HOST = "";

	public static void main(String[] args) {
		try (FileSystem fs = FileSystem.get(new URI("hdfs://" + HDFS_HOST + ":8020/"))) {
			if (!fs.exists(MODEL)) {
				try (JavaSparkContext jsc = Spark.newApp("MMDS Wiki")//.setMaster("local[4]").setWorkerMemory("2g")
						.context()) {
					LDA_Recommender.train(jsc, DOCUMENTS, NUM_TOPICS, ITERATIONS, fs).save(MODEL, fs);
				}
			}
			try (JavaSparkContext jsc = Spark.newApp("MMDS Wiki")//.setMaster("local[4]").setWorkerMemory("2g")
					.context()) {
				Recommender r = LDA_Recommender.load(jsc, MODEL, fs);
				File file = new File(OUT_FILE);
				file.getParentFile().mkdirs();
				if (file.exists()) {
					FileUtils.forceDelete(file);
				}
				Edits edits = new Edits(jsc, TRAINING_DATA, fs);
				try (OutputStream out = new FileOutputStream(file)) {
					Evaluator eval = new Evaluator(r, edits, GROUND_TRUTH, out, fs);
					eval.evaluate(1000, 100, 1L);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

}
