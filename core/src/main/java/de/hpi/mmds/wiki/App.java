package de.hpi.mmds.wiki;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import de.hpi.mmds.wiki.categories.CategoryAnalyzer;
import de.hpi.mmds.wiki.cf.CollaborativeFiltering;
import de.hpi.mmds.wiki.lda.LDA;

public class App {

	private static enum RecommenderType {
		CAT, CF, LDA;

		private static final double SUMMED_WEIGHTS = Arrays.stream(RecommenderType.values())
				.mapToDouble(RecommenderType::weight).sum();

		private final double weight;

		RecommenderType() {
			this(1);
		}

		RecommenderType(double weight) {
			this.weight = weight;
		}

		private double weight() {
			return weight;
		}
	}

	private static final String CF_FILTER_DIR = null;
	private static final String DATA_DIR = null;

	private static List<Recommendation> aggregate(List<Tuple2<RecommenderType, List<Recommendation>>> recommendations) {
		Map<Integer, Map<RecommenderType, Double>> values = new HashMap<>();
		for (Tuple2<RecommenderType, List<Recommendation>> t : recommendations) {
			for (Recommendation r : t._2) {
				Map<RecommenderType, Double> map = values.get(r.getArticle());
				if (map == null) {
					map = new HashMap<>();
					values.put(r.getArticle(), map);
				}
				map.put(t._1, r.getPrediction());
			}
		}
		return values.entrySet().stream().map(e -> new Recommendation(aggregate(e.getValue()), e.getKey()))
				.collect(Collectors.toList());
	}

	private static double aggregate(Map<RecommenderType, Double> values) {
		return values.entrySet().stream().mapToDouble(f -> f.getKey().weight() * f.getValue()).sum()
				/ RecommenderType.SUMMED_WEIGHTS;
	}

	public static void main(String[] args) {
		final int user = 1;
		try (JavaSparkContext jsc = SparkUtil.getContext()) {
			Edits edits = new Edits(jsc, DATA_DIR);
			List<Tuple2<RecommenderType, Recommender>> recommenders = new ArrayList<>();
			recommenders.add(new Tuple2<>(RecommenderType.CF, new CollaborativeFiltering(jsc, CF_FILTER_DIR)));
			recommenders.add(new Tuple2<>(RecommenderType.CAT, new CategoryAnalyzer()));
			recommenders.add(new Tuple2<>(RecommenderType.LDA, new LDA()));
			List<Tuple2<RecommenderType, List<Recommendation>>> recommendations = recommenders.parallelStream()
					.map(t -> new Tuple2<>(t._1, t._2.recommend(user, edits.getEdits(user))))
					.collect(Collectors.toList());
			aggregate(recommendations);
		} catch (Exception e) {

		}
	}
}
