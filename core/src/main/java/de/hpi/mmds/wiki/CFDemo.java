package de.hpi.mmds.wiki;

import de.hpi.mmds.wiki.cf.CollaborativeFiltering;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;

public class CFDemo {

	private static final String NAME = "default_new";
	public static final String DATA_DIR = "data/";
	private static final String FILTER = "filter/" + NAME;
	private static final String TRAINING_DATA = DATA_DIR + "training_new.txt";
	private static final String GROUND_TRUTH = DATA_DIR + "ground_truth_new.csv";
	private static final String OUT_FILE = "log/eval_cf_" + NAME + ".txt";
	private static final int RANK = 25;
	private static final int ITERATIONS = 10;
	private static final double LAMBDA = 0.01;
	private static final double ALPHA = 1.0;

	public static void main(String[] args) {
		try (FileSystem fs = FileSystem.getLocal()) {
			if (!fs.exists(FILTER)) {
				try (JavaSparkContext jsc = Spark.newApp("MMDS Wiki").setMaster("local[4]").setWorkerMemory("2g")
						.context()) {
					long start = System.nanoTime();
					CollaborativeFiltering.train(jsc, TRAINING_DATA, RANK, ITERATIONS, LAMBDA, ALPHA, fs).save(FILTER, fs);
					long duration = System.nanoTime() - start;
					File result = new File("result.txt");
					if(result.exists()) {
						FileUtils.forceDelete(result);
					}
					try(FileWriter out = new FileWriter(result)) {
						out.write("Took " + (duration / 1_000_000) + "ms");
					}
				}
			}
			try (JavaSparkContext jsc = Spark.newApp("MMDS Wiki").setMaster("local[4]").setWorkerMemory("2g")
					.context()) {
				Recommender r = CollaborativeFiltering.load(jsc, FILTER, fs);
				Edits edits = new Edits(jsc, TRAINING_DATA, fs);
				File file = new File(OUT_FILE);
				file.getParentFile().mkdirs();
				if (file.exists()) {
					FileUtils.forceDelete(file);
				}
				try (OutputStream out = new FileOutputStream(file)) {
					Evaluator eval = new Evaluator(r, edits, GROUND_TRUTH, out, fs);
					eval.evaluate(1000, 5000, 1L);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
