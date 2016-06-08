package de.hpi.mmds.wiki;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Evaluator {

	public static final int NUM_RECOMMENDATIONS = 100;
	private final Edits test;
	private final Edits training;
	private final File out;
	private final Recommender recommender;

	public Evaluator(Recommender recommender, Edits test, Edits training, File out) {
		this.recommender = recommender;
		this.training = training;
		this.test = test;
		this.out = out;
	}

	public void evaluate(int num, long seed) {
		List<Integer> uids = new ArrayList<>(test.getUsers().intersection(training.getUsers()).collect());
		int i = 0;
		double totalPrecision = 0.0;
		double totalRecall = 0.0;
		try {
			FileUtils.forceDelete(out);
		} catch (IOException e) {
			throw new RuntimeException("Could not delete output file " + out.getPath(), e);
		}
		Collections.shuffle(uids, new Random(seed));
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(this.out))) {
			for (int user : uids) {
				if (i >= num) {
					break;
				}
				JavaRDD<Integer> articles = training.getEdits(user);
				List<Integer> a = test.getEdits(user).collect();
				List<Integer> p = articles.collect();
				Set<Integer> groundTruth = new HashSet<>(a);
				groundTruth.removeAll(p);
				if (!groundTruth.isEmpty()) {
					List<Integer> recommendations = recommender.recommend(user, articles, NUM_RECOMMENDATIONS).stream()
							.map(Recommendation::getArticle).collect(Collectors.toList());
					Set<Integer> intersect = new HashSet<>(groundTruth);
					intersect.retainAll(recommendations);
					double precision = recommendations.isEmpty() ?
							0 :
							(double) intersect.size() / recommendations.size();
					double recall = (double) intersect.size() / groundTruth.size();
					totalPrecision += precision;
					totalRecall += recall;
					i++;
					writer.write("User: " + user + "\n");
					writer.write("Recommendations: " + recommendations + "\n");
					writer.write("Gold standard: " + groundTruth + "\n");
					writer.write("Matches: " + intersect + "\n");
					writer.write("Precision: " + precision + "\n");
					writer.write("AVG Precision: " + totalPrecision / i + "\n");
					writer.write("Recall: " + recall + "\n");
					writer.write("AVG Recall: " + totalRecall / i + "\n");
					writer.write("Processed: " + i + "\n");
					writer.write("---\n");
					writer.flush();
				}
			}
		} catch (IOException e) {
			throw new RuntimeException("Error writing to output file " + out.getPath(), e);
		}
		if (i > 0) {
			System.out.println("AVG Precision: " + totalPrecision / i);
			System.out.println("AVG Recall: " + totalRecall / i);
		}
	}

	public void evaluate(int num) {
		evaluate(num, 1L);
	}
}
