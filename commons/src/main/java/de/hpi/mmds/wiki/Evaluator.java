package de.hpi.mmds.wiki;

import com.google.common.collect.Sets;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Evaluator {

	private static final Logger LOGGER = Logger.getLogger(Evaluator.class.getName());
	private final Edits test;
	private final Edits training;
	private final File out;
	private final Recommender recommender;

	public Evaluator(Recommender recommender, Edits test, Edits training, File out) {
		this.recommender = recommender;
		this.training = training.cache();
		this.test = test;
		this.out = out;
	}

	public Map<Integer, Result> evaluate(int num, int howMany) {
		return evaluate(num, howMany, new Random().nextLong());
	}

	public Map<Integer, Result> evaluate(int num, int howMany, long seed) {
		Map<Integer, Result> results = new HashMap<>();
		int i = 0;
		JavaPairRDD<Integer, Set<Integer>> groundTruths = test.getAggregatedEdits().join(training.getAggregatedEdits())
				.mapValues(t -> (Set<Integer>) Sets.difference(Sets.newHashSet(t._1), Sets.newHashSet(t._2))
						.immutableCopy()).filter(t -> !t._2.isEmpty());
		double totalPrecision = 0.0;
		double totalMAP = 0.0;
		double totalRecall = 0.0;
		double totalFmeasure = 0.0;
		File parentFile = out.getParentFile();
		if (parentFile != null) {
			parentFile.mkdirs();
		}
		if (out.exists()) {
			try {
				FileUtils.forceDelete(out);
			} catch (IOException e) {
				throw new RuntimeException("Could not delete output file " + out.getPath(), e);
			}
		}
		LOGGER.info("Sampling " + num + " out of " + groundTruths.count() + " users");
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(out))) {
			for (Tuple2<Integer, Set<Integer>> t : groundTruths.takeSample(false, num, seed)) {
				int user = t._1;
				JavaRDD<Integer> articles = training.getEdits(user);
				Set<Integer> groundTruth = t._2;
				List<Integer> recommendations = recommender.recommend(user, articles, howMany).stream()
						.map(Recommendation::getArticle).collect(Collectors.toList());
				Result<Integer> result = new Result<>(recommendations, groundTruth);
				results.put(user, result);
				totalPrecision += result.precision();
				totalMAP += result.meanAveragePrecision();
				totalRecall += result.recall();
				totalFmeasure += result.fmeasure();
				i++;
				int median = results.size() / 2;
				writer.write("User: " + user + "\n");
				writer.write(result.printResult());
				writer.newLine();
				writer.write("AVG Precision: " + totalPrecision / i + "\n");
				writer.write("AVG MAP: " + totalMAP / i + "\n");
				writer.write("AVG Recall: " + totalRecall / i + "\n");
				writer.write("AVG F-Measure: " + totalFmeasure / i + "\n");
				writer.write("Median Precision: " + results.values().stream().map(Result::precision).sorted()
						.collect(Collectors.toList()).get(median) + "\n");
				writer.write("Median MAP: " + results.values().stream().map(Result::meanAveragePrecision).sorted()
						.collect(Collectors.toList()).get(median) + "\n");
				writer.write("Median Recall: " + results.values().stream().map(Result::recall).sorted()
						.collect(Collectors.toList()).get(median) + "\n");
				writer.write("Median F-Measure: " + results.values().stream().map(Result::fmeasure).sorted()
						.collect(Collectors.toList()).get(median) + "\n");
				writer.write("Processed: " + i + "\n");
				writer.write("---");
				writer.newLine();
				writer.flush();
			}
		} catch (IOException e) {
			throw new RuntimeException("Error writing to output file " + out.getPath(), e);
		}
		if (!results.isEmpty()) {
			int median = results.size() / 2;
			System.out.println("AVG Precision: " + results.values().stream().mapToDouble(Result::precision).average()
					.getAsDouble());
			System.out.println(
					"AVG MAP: " + results.values().stream().mapToDouble(Result::meanAveragePrecision).average()
							.getAsDouble());
			System.out.println(
					"AVG Recall: " + results.values().stream().mapToDouble(Result::recall).average().getAsDouble());
			System.out.println("AVG F-Measure: " + results.values().stream().mapToDouble(Result::fmeasure).average()
					.getAsDouble());
			System.out.println("Median Precision: " + results.values().stream().map(Result::precision).sorted()
					.collect(Collectors.toList()).get(median));
			System.out.println("Median MAP: " + results.values().stream().map(Result::meanAveragePrecision).sorted()
					.collect(Collectors.toList()).get(median));
			System.out.println("Median Recall: " + results.values().stream().map(Result::recall).sorted()
					.collect(Collectors.toList()).get(median));
			System.out.println("Median F-Measure: " + results.values().stream().map(Result::fmeasure).sorted()
					.collect(Collectors.toList()).get(median));
		}
		return results;
	}

	public static class Result<T> {

		private final List<T> recommendations;
		private final Set<T> groundTruth;
		private final Set<T> intersect;

		public Result(List<T> recommendations, Set<T> groundTruth) {
			this.recommendations = recommendations;
			this.groundTruth = groundTruth;
			this.intersect = new HashSet<>(groundTruth);
			this.intersect.retainAll(recommendations);
		}

		public double fmeasure() {
			double recall = recall();
			double precision = precision();
			return recall + precision == 0.0 ? 0.0 : 2 * recall * precision / (recall + precision);
		}

		public double meanAveragePrecision() {
			double map = 0.0;
			int tp = 0;
			int i = 0;
			for (T recommendation : recommendations) {
				i++;
				if (groundTruth.contains(recommendation)) {
					tp++;
					map += (double) tp / i;
				}
			}
			return tp == 0 ? 0.0 : map / tp;
		}

		public double precision() {
			return recommendations.isEmpty() ? 0 : (double) intersect.size() / recommendations.size();
		}

		public String printResult() {
			return "Recommendations: " + recommendations + "\n" +
					"Gold standard: " + groundTruth + "\n" +
					"Matches: " + intersect + "\n" +
					"Precision: " + precision() + "\n" +
					"Mean Average Precision: " + meanAveragePrecision() + "\n" +
					"Recall: " + recall() + "\n" +
					"F-Measure: " + fmeasure();
		}

		public double recall() {
			return (double) intersect.size() / groundTruth.size();
		}
	}
}
