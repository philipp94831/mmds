package de.hpi.mmds.wiki;

import de.hpi.mmds.wiki.cf.CollaborativeFiltering;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class CFDemo {

	private static final String NAME = "only_counts";
	private static final String FILTER = "filter/" + NAME;
	private static final String TRAINING_DATA = "data/edits_3/training*.txt";
	private static final String GROUND_TRUTH = "data/ground_truth.csv";
	private static final String OUT_FILE = "log/eval_" + NAME + ".txt";
	private static final int RANK = 25;
	private static final int ITERATIONS = 10;
	private static final double LAMBDA = 0.01;
	private static final double ALPHA = 1.0;

	public static void main(String[] args) {
		try (FileSystem fs = FileSystem.getLocal()) {
			if (!fs.exists(FILTER)) {
				try (JavaSparkContext jsc = Spark.newApp("MMDS Wiki").setMaster("local[4]").setWorkerMemory("2g")
						.context()) {
					CollaborativeFiltering.train(jsc, TRAINING_DATA, RANK, ITERATIONS, LAMBDA, ALPHA, fs).save(FILTER, fs);
				}
			}
			try (JavaSparkContext jsc = Spark.newApp("MMDS Wiki").setMaster("local[4]").setWorkerMemory("2g")
					.context()) {
				Recommender r = CollaborativeFiltering.load(jsc, FILTER, fs);
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
		}
	}
}
