package de.hpi.mmds.wiki;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;

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

	public static class Result<T> {

		private final Set<T> recommendations;
		private final Set<T> groundTruth;
		private final Set<T> intersect;

		public Result(Set<T> recommendations, Set<T> groundTruth) {
			this.recommendations = recommendations;
			this.groundTruth = groundTruth;
			this.intersect = new HashSet<>(groundTruth);
			this.intersect.retainAll(recommendations);
		}

		public double precision() {
			return recommendations.isEmpty() ? 0 : (double) intersect.size() / recommendations.size();
		}

		public double recall() {
			return (double) intersect.size() / groundTruth.size();
		}

		public String printResult() {
			StringBuilder sb = new StringBuilder();
			sb.append("Recommendations: " + recommendations + "\n");
			sb.append("Gold standard: " + groundTruth + "\n");
			sb.append("Matches: " + intersect + "\n");
			sb.append("Precision: " + precision() + "\n");
			sb.append("Recall: " + recall());
			return sb.toString();
		}
	}

	public Evaluator(Recommender recommender, Edits test, Edits training, File out) {
		this.recommender = recommender;
		this.training = training;
		this.test = test;
		this.out = out;
	}

	public Map<Integer, Result> evaluate(int num, long seed) {
		Map<Integer, Result> results = new HashMap<>();
		List<Integer> uids = new ArrayList<>(test.getUsers().intersection(training.getUsers()).collect());
		int i = 0;
		double totalPrecision = 0.0;
		double totalRecall = 0.0;
		if (out.exists()) {
			try {
				FileUtils.forceDelete(out);
			} catch (IOException e) {
				throw new RuntimeException("Could not delete output file " + out.getPath(), e);
			}
		}
		Collections.shuffle(uids, new Random(seed));
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(out))) {
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
					Set<Integer> recommendations = recommender.recommend(user, articles, NUM_RECOMMENDATIONS).stream()
							.map(Recommendation::getArticle).collect(Collectors.toSet());
					Result<Integer> result = new Result<>(recommendations, groundTruth);
					results.put(user, result);
					totalPrecision += result.precision();
					totalRecall += result.recall();
					i++;
					writer.write("User: " + user + "\n");
					writer.write(result.printResult());
					writer.newLine();
					writer.write("AVG Precision: " + totalPrecision / i + "\n");
					writer.write("AVG Recall: " + totalRecall / i + "\n");
					writer.write("Processed: " + i + "\n");
					writer.write("---");
					writer.newLine();
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
		return results;
	}

	public Map<Integer, Result> evaluate(int num) {
		return evaluate(num, 1L);
	}
}
