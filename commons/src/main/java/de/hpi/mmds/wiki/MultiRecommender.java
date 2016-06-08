package de.hpi.mmds.wiki;

import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MultiRecommender implements Recommender {

	private final List<Tuple2<Double, Recommender>> recommenders = new ArrayList<>();
	private double summedWeights = 0.0;

	public void add(double weight, Recommender recommender) {
		summedWeights += weight;
		recommenders.add(new Tuple2<>(weight, recommender));
	}

	@Override
	public List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany) {
		List<Tuple2<Double, List<Recommendation>>> recommendations = recommenders.parallelStream()
				.map(t -> new Tuple2<>(t._1, t._2.recommend(userId, articles))).collect(Collectors.toList());
		return aggregate(recommendations);
	}

	private List<Recommendation> aggregate(List<Tuple2<Double, List<Recommendation>>> recommendations) {
		Map<Integer, List<Tuple2<Double, Double>>> values = new HashMap<>();
		for (Tuple2<Double, List<Recommendation>> t : recommendations) {
			for (Recommendation r : t._2) {
				List<Tuple2<Double, Double>> list = values.get(r.getArticle());
				if (list == null) {
					list = new ArrayList<>();
					values.put(r.getArticle(), list);
				}
				list.add(new Tuple2<>(t._1, r.getPrediction()));
			}
		}
		return values.entrySet().stream().map(e -> new Recommendation(sum(e.getValue()), e.getKey()))
				.collect(Collectors.toList());
	}

	private double sum(List<Tuple2<Double, Double>> values) {
		return values.stream().mapToDouble(t -> t._1 * t._2).sum() / summedWeights;
	}
}
