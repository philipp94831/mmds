package de.hpi.mmds.wiki;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import de.hpi.mmds.wiki.cf.CollaborativeFiltering;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;

public class CFDemo {

	@Parameter(names = "-fs", description = "File system to use. May be either an HDFS URL or local", required = true)
	private String uri = "local";
	@Parameter(names = "--help", help = true)
	private boolean help = false;

	@Parameter(names = "-path", description = "Path to save and read model from", required = true)
	private String FILTER;
	@Parameter(names = "-data", description = "Path to edit data")
	private String DATA;
	@Parameter(names = "-rank", description = "Rank of matrix to use for CF")
	private int RANK = 25;
	@Parameter(names = "-iterations", description = "Number of iterations for CF")
	private int ITERATIONS = 10;
	@Parameter(names = "-lambda", description = "Regularization parameter for CF")
	private double LAMBDA = 0.01;
	@Parameter(names = "-alpha", description = "Weighting parameter for CF")
	private double ALPHA = 1.0;

	@Parameter(names = "-history", description = "Path to historical data to use for evaluation")
	private String TRAINING_DATA;
	@Parameter(names = "-test", description = "Path to test data for evaluation")
	private String TEST_DATA;
	@Parameter(names = "-log", description = "Path to file where evaluation results should be logged")
	private String OUT_FILE;
	@Parameter(names = "-evaluate", description = "Evaluate model using ground truth and historical data")
	private boolean evaluate = false;

	public static void main(String[] args) {
		CFDemo demo = new CFDemo();
		JCommander jc = new JCommander(demo, args);
		if(demo.help) {
			jc.usage();
			System.exit(0);
		}
		demo.run();
	}

	private void run() {
		try (FileSystem fs = FileSystem.get(uri)) {
			if (!fs.exists(FILTER)) {
				try (JavaSparkContext jsc = Spark.newApp("MMDS Wiki").setMaster("local[4]").setWorkerMemory("2g")
						.context()) {
					long start = System.nanoTime();
					CollaborativeFiltering.train(jsc, DATA, RANK, ITERATIONS, LAMBDA, ALPHA, fs).save(FILTER, fs);
					long duration = System.nanoTime() - start;
					System.out.println("Took " + (duration / 1_000_000) + "ms");
				}
			}
			if (evaluate) {
				try (JavaSparkContext jsc = Spark.newApp("MMDS Wiki").setMaster("local[4]").setWorkerMemory("2g")
						.context()) {
					Edits edits = new Edits(jsc, TRAINING_DATA, fs);
					Edits test = new Edits(jsc, TEST_DATA, fs);
					Recommender r = CollaborativeFiltering.load(jsc, FILTER, fs);
					File file = new File(OUT_FILE);
					new EvaluatorDemo(file, r, test, edits).run();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
