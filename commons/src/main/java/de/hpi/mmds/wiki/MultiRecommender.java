package de.hpi.mmds.wiki;

import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
		Map<Integer, List<Double>> values = new HashMap<>();
		for (Tuple2<Double, List<Recommendation>> t : recommendations) {
			for (Recommendation r : t._2) {
				List<Double> list = values.get(r.getArticle());
				if (list == null) {
					list = new ArrayList<>();
					values.put(r.getArticle(), list);
				}
				list.add(t._1 * r.getPrediction());
			}
		}
		return values.entrySet().stream().map(e -> new Recommendation(avg(e.getValue()), e.getKey())).sorted()
				.collect(Collectors.toList());
	}

	private double avg(List<Double> values) {
		return values.stream().mapToDouble(d -> d).sum() / summedWeights;
	}

	@Override
	public List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany) {
		List<Tuple2<Double, List<Recommendation>>> recommendations = recommenders.parallelStream()
				.map(t -> new Tuple2<>(t._1, t._2.recommend(userId, articles))).collect(Collectors.toList());
		return aggregate(recommendations);
	}
}
