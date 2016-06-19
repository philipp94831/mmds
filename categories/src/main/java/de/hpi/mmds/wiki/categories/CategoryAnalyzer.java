package de.hpi.mmds.wiki.categories;

import de.hpi.mmds.wiki.HDFS;
import de.hpi.mmds.wiki.Recommendation;
import de.hpi.mmds.wiki.Recommender;
import de.hpi.mmds.wiki.spark.TransitiveClosure;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class CategoryAnalyzer implements Recommender {

	private final JavaPairRDD<Integer, Set<Recommendation>> similarities;

	public CategoryAnalyzer(JavaPairRDD<Integer, Set<Recommendation>> similarities) {
		this.similarities = similarities.cache();
	}

	@Override
	public List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany) {
		long numArticles = articles.cache().count();
		List<Recommendation> recommendations = similarities
				.join(articles.mapToPair(article -> new Tuple2<>(article, null))).flatMap(t -> t._2._1)
				.mapToPair(r -> new Tuple2<>(r.getArticle(), r.getPrediction())).reduceByKey(Double::sum)
				.mapValues(d -> d / numArticles).map(t -> new Recommendation(t._2, t._1)).collect();
		return recommendations.stream().sorted().limit(howMany).collect(Collectors.toList());
	}

	public static CategoryAnalyzer build(JavaSparkContext jsc, String treeData, String articleData) {
		// Loads 'subcat' categoryLinks into tuples: (parent_category_id, subcategory_id)
		JavaRDD<Tuple2<Integer, Integer>> edges = jsc.textFile(treeData).map(CategoryAnalyzer::parseParentCategories);

		// Performs transitive closure, e.g. if there are tuples (1,2) and (2,3), it will add a new tuple (1,3) to list
		JavaPairRDD<Integer, Integer> transitiveCategories = TransitiveClosure.compute(edges)
				.flatMap(t -> Arrays.asList(t, new Tuple2<>(t._1, t._1))).mapToPair(t -> t);

		// Loads 'page' categoryLinks into tuples: (category_id, article_id) and cashes them
		JavaPairRDD<Integer, Integer> articleCategories = jsc.textFile(articleData)
				.mapToPair(CategoryAnalyzer::parseArticleCategories).cache();

		// Produces pairs (category, article), which means that article is in category
		JavaPairRDD<Integer, Integer> transitiveCategoriesForArticles = articleCategories.join(transitiveCategories)
				.mapToPair(t -> t._2).cache();

		long articleCount = articleCategories.values().count();

		// Computes information gain for each category, given by formula G = 1 + x*log(2,x) + y*log(2,y))
		//   where 'x' is number of articles in given category and 'y' the overall number of articles
		// TODO: use actual I.G. formula instead of simple average
		JavaPairRDD<Integer, Double> informationGains = transitiveCategoriesForArticles.mapValues(i -> 1)
				.reduceByKey(Integer::sum).filter(t -> t._2 > 0).mapValues(i -> 1.0 - (double) i / articleCount);

		// Creates tuple (article_id, ((category_id, category_information_gain), ...)) for each article, where category
		//   vector will contain information gain of category, if article is in category, or 0 otherwise
		JavaPairRDD<Integer, Iterable<Tuple2<Integer, Double>>> categoriesPerArticle = transitiveCategoriesForArticles
				.join(informationGains).mapToPair(t -> new Tuple2<>(t._2._1, new Tuple2<>(t._1, t._2._2))).groupByKey();

		return new CategoryAnalyzer(setSimilarityJoin(categoriesPerArticle));
	}

	private static Tuple2<Integer, Integer> parseArticleCategories(String v1) {
		// category,article
		String[] split = v1.split(",");
		if (split.length != 2) {
			throw new IllegalStateException("Data malformed");
		}
		return new Tuple2<>(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
	}

	private static Tuple2<Integer, Integer> parseParentCategories(String v1) {
		// parent,child,#artices,#subcats
		String[] split = v1.split(",");
		if (split.length < 2) {
			throw new IllegalStateException("Data malformed");
		}
		return new Tuple2<>(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
	}

	private static JavaPairRDD<Integer, Set<Recommendation>> setSimilarityJoin(
			JavaPairRDD<Integer, Iterable<Tuple2<Integer, Double>>> articles) {
		// TODO: Set Similarity join, Jaccard similarity
		return JavaSparkContext.fromSparkContext(articles.context()).emptyRDD()
				.mapToPair(o -> new Tuple2<>(-1, Collections.emptySet()));
	}

	public CategoryAnalyzer save(String saveLocation, HDFS fs) throws IOException {
		fs.delete(new Path(saveLocation));
		similarities.saveAsObjectFile(saveLocation);
		return this;
	}

	public static CategoryAnalyzer load(JavaSparkContext jsc, String saveLocation) throws IOException {
		JavaPairRDD<Integer, Set<Recommendation>> similarities = JavaPairRDD.fromJavaRDD(jsc.objectFile(saveLocation));
		return new CategoryAnalyzer(similarities);
	}

}
