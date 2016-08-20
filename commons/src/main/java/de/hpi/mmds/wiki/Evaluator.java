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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
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
				.textFile(fs.makeQualified(ground_truth).toString()).mapToPair(Evaluator::parseGroundTruth)
				.mapValues(i -> {
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
		try (BufferedWriter out = fs.create(path)) {
			Iterator<Tuple2<Integer, Integer>> it = groundTruths.flatMapValues(s -> s).toLocalIterator();
			while (it.hasNext()) {
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
		long time;
		//LOGGER.info("Sampling " + num + " out of " + groundTruths.count() + " users");
		try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {
			long start = System.nanoTime();
			for (Tuple2<Integer, Set<Integer>> t : groundTruths.takeSample(false, num, seed)) {
				int user = t._1;
				JavaRDD<Integer> articles = training.getEdits(user);
				Set<Integer> groundTruth = t._2;
				long s = System.nanoTime();
				List<Integer> recommendations = recommender.recommend(user, articles, howMany).stream()
						.map(Recommendation::getArticle).collect(Collectors.toList());
				long recomTime = System.nanoTime() - s;
				Result<Integer> result = new Result<>(recommendations, groundTruth, recomTime / 1_000_000);
				results.put(user, result);
				i++;
				writer.write("User: " + user + "\n");
				writer.write("History: " + articles.collect());
				writer.newLine();
				writer.write(result.printResult());
				writer.newLine();
				writer.write("AVG Precision: " + average(results.values(), Result::precision) + "\n");
				writer.write("AVG MAP: " + average(results.values(), Result::meanAveragePrecision) + "\n");
				writer.write("AVG Recall: " + average(results.values(), Result::recall) + "\n");
				writer.write("AVG Recall at 100: " + average(results.values(), r -> r.recall(100)) + "\n");
				writer.write("AVG Recall at 500: " + average(results.values(), r -> r.recall(500)) + "\n");
				writer.write("AVG Recall at 1000: " + average(results.values(), r -> r.recall(1000)) + "\n");
				writer.write("AVG F-Measure: " + average(results.values(), Result::fmeasure) + "\n");
				writer.write("AVG Time: " + average(results.values(), Result::getTime) + "\n");
				writer.write("Median Time: " + median(results.values(), Result::getTime) + "\n");
				writer.write("Median Precision: " + median(results.values(), Result::precision) + "\n");
				writer.write("Median MAP: " + median(results.values(), Result::meanAveragePrecision) + "\n");
				writer.write("Median Recall: " + median(results.values(), Result::recall) + "\n");
				writer.write("Median F-Measure: " + median(results.values(), Result::fmeasure) + "\n");
				writer.write("Processed: " + i + "\n");
				writer.write("---");
				writer.newLine();
			}
			time = (System.nanoTime() - start) / 1_000_000;
			writer.newLine();
			writer.write("Evaluation took " + time + "ms");
		} catch (IOException e) {
			throw new RuntimeException("Error writing to output stream", e);
		}
		LOGGER.info("Evaluation took " + time + "ms");
		if (!results.isEmpty()) {
			LOGGER.info("AVG Precision: " + average(results.values(), Result::precision));
			LOGGER.info("AVG MAP: " + average(results.values(), Result::meanAveragePrecision));
			LOGGER.info("AVG Recall: " + average(results.values(), Result::recall));
			LOGGER.info("AVG F-Measure: " + average(results.values(), Result::fmeasure));
			LOGGER.info("Median Precision: " + median(results.values(), Result::precision));
			LOGGER.info("Median MAP: " + median(results.values(), Result::meanAveragePrecision));
			LOGGER.info("Median Recall: " + median(results.values(), Result::recall));
			LOGGER.info("Median F-Measure: " + median(results.values(), Result::fmeasure));
		}
		return results;
	}

	private static <T, U extends Comparable<U>> U median(Collection<T> values, Function<T, U> f) {
		int median = values.size() / 2;
		return values.stream().map(f).sorted().collect(Collectors.toList()).get(median);
	}

	private static <T> double average(Collection<T> values, ToDoubleFunction<T> f) {
		return values.stream().mapToDouble(f).average().getAsDouble();
	}

	public static class Result<T> {

		private final List<T> recommendations;
		private final Set<T> groundTruth;
		private final Set<T> intersect;
		private final long time;

		public Result(List<T> recommendations, Set<T> groundTruth, long time) {
			this.recommendations = recommendations;
			this.groundTruth = groundTruth;
			this.intersect = new HashSet<>(groundTruth);
			this.intersect.retainAll(recommendations);
			this.time = time;
		}

		private long getTime() {
			return time;
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

		public double recall(int at) {
			Set<T> intersect = recommendations.stream().limit(at).collect(Collectors.toSet());
			intersect.retainAll(groundTruth);
			return (double) intersect.size() / groundTruth.size();
		}

		public double recall() {
			return (double) intersect.size() / groundTruth.size();
		}
	}
}
