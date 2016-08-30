package de.hpi.mmds.wiki;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import de.hpi.mmds.wiki.lda.LDARecommender;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;

public class LDADemo {

	@Parameter(names = "-fs", description = "File system to use. May be either an HDFS URL or local", required = true)
	private String uri = "local";
	@Parameter(names = "--help", help = true)
	private boolean help = false;

	@Parameter(names = "-path", description = "Path to save and read model from", required = true)
	private String MODEL;
	@Parameter(names = "-data", description = "Path to document data")
	private String DOCUMENTS;
	@Parameter(names = "-num-topics", description = "Number of topics to use for LDA")
	private int NUM_TOPICS = 400;
	@Parameter(names = "-iterations", description = "Number of iterations for LDA")
	private int ITERATIONS = 10;
	@Parameter(names = "-sample", description = "Ration how many documents should be used for model training. Must be in interval [0.0, 1.0]")
	private double sampleSize = 1.0;

	@Parameter(names = "-history", description = "Path to historical data to use for evaluation")
	private String TRAINING_DATA;
	@Parameter(names = "-test", description = "Path to test data for evaluation")
	private String TEST_DATA;
	@Parameter(names = "-log", description = "Path to file where evaluation results should be logged")
	private String OUT_FILE;
	@Parameter(names = "-evaluate", description = "Evaluate model using ground truth and historical data")
	private boolean evaluate = false;

	public static void main(String[] args) {
		LDADemo demo = new LDADemo();
		JCommander jc = new JCommander(demo, args);
		if(demo.help) {
			jc.usage();
			System.exit(0);
		}
		demo.run();
	}

	private void run() {
		try (FileSystem fs = FileSystem.get(uri)) {
			if (!fs.exists(MODEL)) {
				try (JavaSparkContext jsc = Spark.newApp("MMDS Wiki").setMaster("local[4]").setWorkerMemory("1g")
						.setResultSize("6g").context()) {
					LDARecommender.train(jsc, DOCUMENTS, NUM_TOPICS, ITERATIONS, sampleSize, fs).save(MODEL, fs);
				}
			}
			if (evaluate) {
				try (JavaSparkContext jsc = Spark.newApp("MMDS Wiki").setMaster("local[4]").setWorkerMemory("2g")
						.context()) {
					Recommender r = LDARecommender.load(jsc, MODEL, fs);
					Edits edits = new Edits(jsc, TRAINING_DATA, fs);
					Edits test = new Edits(jsc, TEST_DATA, fs);
					File file = new File(OUT_FILE);
					new EvaluatorDemo(file, r, test, edits).run();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
