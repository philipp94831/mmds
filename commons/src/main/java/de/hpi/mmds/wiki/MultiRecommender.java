package de.hpi.mmds.wiki;

import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link Recommender} composed of multiple other recommenders. Aggregates the result of each recommender by using the
 * average. Different recommenders may be weighted differently.
 */
public class MultiRecommender implements Recommender {

	public static final double DEFAULT_WEIGHT = 1.0;
	private final List<Tuple2<Double, Recommender>> recommenders = new ArrayList<>();
	private double summedWeights = 0.0;

	/**
	 * Add a recommender with a certain weight.
	 *
	 * @param weight
	 * 		double specifying the weight which should be applied for recommendations from this recommender
	 * @param recommender
	 * 		recommender to add
	 *
	 * @return Returns {@code this} to enable chaining.
	 */
	public MultiRecommender add(double weight, Recommender recommender) {
		summedWeights += weight;
		recommenders.add(new Tuple2<>(weight, recommender));
		return this;
	}

	/**
	 * Add a recommender with the {@link #DEFAULT_WEIGHT}.
	 *
	 * @param recommender
	 * 		recommender to add
	 *
	 * @return Returns {@code this} to enable chaining.
	 *
	 * @see #DEFAULT_WEIGHT
	 */
	public MultiRecommender add(Recommender recommender) {
		return add(DEFAULT_WEIGHT, recommender);
	}

	private List<Recommendation> aggregate(List<Tuple2<Double, List<Recommendation>>> recommendations) {
		Set<Integer> articles = new HashSet<>();
		List<Map<Integer, Double>> scores = new ArrayList<>();
		List<Double> weights = new ArrayList<>();
		List<Double> mins = new ArrayList<>();
		List<Double> maxs = new ArrayList<>();
		for (Tuple2<Double, List<Recommendation>> t : recommendations) {
			Map<Integer, Double> recom = new HashMap<>();
			scores.add(recom);
			double min = Double.MAX_VALUE;
			double max = Double.MIN_VALUE;
			for (Recommendation r : t._2) {
				int article = r.getArticle();
				articles.add(article);
				double value = r.getPrediction();
				if(value < min) {
					min = value;
				}
				if(value > max) {
					max = value;
				}
				recom.put(article, value);
			}
			weights.add(t._1());
			mins.add(min);
			maxs.add(max);
		}
		Map<Integer, Double> values = new HashMap<>();
		for(int article : articles) {
			double res = 0.0;
			int i = 0;
			for(Map<Integer, Double> score : scores) {
				double r = score.getOrDefault(article, 0.99 * mins.get(i));
				res += r / maxs.get(i) * weights.get(i);
				i++;
			}
			values.put(article, res);
		}
		return values.entrySet().stream().map(e -> new Recommendation(e.getValue(), e.getKey())).sorted()
				.collect(Collectors.toList());
	}

	private double avg(List<Double> values) {
		return values.stream().mapToDouble(d -> d).sum() / summedWeights;
	}

	@Override
	public List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany) {
		List<Tuple2<Double, List<Recommendation>>> recommendations = recommenders.stream()
				.map(t -> new Tuple2<>(t._1, t._2.recommend(userId, articles, 3 * howMany)))
				.collect(Collectors.toList());
		return aggregate(recommendations).stream().limit(howMany).collect(Collectors.toList());
	}
}
