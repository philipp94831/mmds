package de.hpi.mmds.wiki;

import com.google.common.collect.Sets;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static de.hpi.mmds.wiki.spark.SparkFunctions.swap;

public class Evaluator {

	private static final Logger LOGGER = Logger.getLogger(Evaluator.class.getName());
	private final Edits training;
	private final OutputStream out;
	private final Recommender recommender;
	private final JavaPairRDD<Integer, Set<Integer>> groundTruths;

	public Evaluator(Recommender recommender, Edits training, String ground_truth, OutputStream out, FileSystem fs) {
		this.recommender = recommender;
		this.training = training.cache();
		this.groundTruths = JavaSparkContext.fromSparkContext(training.getAggregatedEdits().context())
				.textFile(fs.makeQualified(ground_truth).toString()).mapToPair(Evaluator::parseGroundTruth).mapValues(i -> {
					Set<Integer> s = new HashSet<>();
					s.add(i);
					return s;
				}).reduceByKey((s1, s2) -> {
					s1.addAll(s2);
					return s1;
				});
		this.out = out;
	}

	public Evaluator(Recommender recommender, Edits test, Edits training, OutputStream out) {
		this.recommender = recommender;
		this.training = training.cache();
		this.groundTruths = generateGroundTruth(test, training);
		this.out = out;
	}

	public static JavaPairRDD<Integer, Set<Integer>> generateGroundTruth(Edits test, Edits training) {
		JavaPairRDD<Integer, Iterable<Integer>> possibleRecommendations = test.getAllEdits().mapToPair(swap())
				.join(training.getArticles().mapToPair(article -> new Tuple2<>(article, null)))
				.mapToPair(t -> new Tuple2<>(t._2._1, t._1)).groupByKey();
		return possibleRecommendations.join(training.getAggregatedEdits()).mapValues(
				t -> (Set<Integer>) Sets.difference(Sets.newHashSet(t._1), Sets.newHashSet(t._2)).immutableCopy())
				.filter(t -> !t._2.isEmpty());
	}

	public void saveGroundTruth(String path, FileSystem fs) throws IOException {
		try(BufferedWriter out = fs.create(path)) {
			Iterator<Tuple2<Integer, Integer>> it = groundTruths.flatMapValues(s -> s).toLocalIterator();
			while(it.hasNext()) {
				Tuple2<Integer, Integer> t = it.next();
				out.write(t._1() + "," + t._2());
				out.newLine();
			}
		}
	}

	private static Tuple2<Integer, Integer> parseGroundTruth(String s) {
		String[] split = s.split(",");
		if (split.length != 2) {
			throw new IllegalStateException("Data malformed");
		}
		return new Tuple2<>(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
	}

	public Map<Integer, Result> evaluate(int num, int howMany) {
		return evaluate(num, howMany, new Random().nextLong());
	}

	public Map<Integer, Result> evaluate(int num, int howMany, long seed) {
		Map<Integer, Result> results = new HashMap<>();
		int i = 0;
		double totalPrecision = 0.0;
		double totalMAP = 0.0;
		double totalRecall = 0.0;
		double totalFmeasure = 0.0;
		long time;
		//LOGGER.info("Sampling " + num + " out of " + groundTruths.count() + " users");
		try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {
			long start = System.nanoTime();
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
//				writer.flush();
			}
			time = (System.nanoTime() - start) / 1_000_000;
			writer.newLine();
			writer.write("Evaluation took " + time + "ms");
		} catch (IOException e) {
			throw new RuntimeException("Error writing to output stream", e);
		}
		LOGGER.info("Evaluation took " + time + "ms");
		if (!results.isEmpty()) {
			int median = results.size() / 2;
			LOGGER.info("AVG Precision: " + results.values().stream().mapToDouble(Result::precision).average()
					.getAsDouble());
			LOGGER.info("AVG MAP: " + results.values().stream().mapToDouble(Result::meanAveragePrecision).average()
					.getAsDouble());
			LOGGER.info("AVG Recall: " + results.values().stream().mapToDouble(Result::recall).average().getAsDouble());
			LOGGER.info("AVG F-Measure: " + results.values().stream().mapToDouble(Result::fmeasure).average()
					.getAsDouble());
			LOGGER.info("Median Precision: " + results.values().stream().map(Result::precision).sorted()
					.collect(Collectors.toList()).get(median));
			LOGGER.info("Median MAP: " + results.values().stream().map(Result::meanAveragePrecision).sorted()
					.collect(Collectors.toList()).get(median));
			LOGGER.info("Median Recall: " + results.values().stream().map(Result::recall).sorted()
					.collect(Collectors.toList()).get(median));
			LOGGER.info("Median F-Measure: " + results.values().stream().map(Result::fmeasure).sorted()
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
