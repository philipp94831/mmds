package de.hpi.mmds.wiki.lda;

// import our stuff

import de.hpi.mmds.wiki.FileSystem;
import de.hpi.mmds.wiki.Recommendation;
import de.hpi.mmds.wiki.Recommender;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class LDA_Recommender implements Serializable, Recommender {

	private static final int TOP_TOPICS = 3;
	private final transient DistributedLDAModel model;
	private static final long serialVersionUID = 5290472017062948753L;

	@Override
	public List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany) {
		return recommend(articles.first(), howMany);
	}

	public List<Recommendation> recommend(long article, int howMany) {
		JavaRDD<Tuple3<Long, int[], double[]>> topics = model.javaTopTopicsPerDocument(TOP_TOPICS)
				.filter(tuple -> tuple._1() == article);
		if (!topics.isEmpty()) {
			Tuple3<Long, int[], double[]> allTopics = topics.first();
			Map<Integer, Double> topicWeights = new HashMap<>();
			for (int i = 0; i < allTopics._2().length; i++) {
				topicWeights.put(allTopics._2()[i], allTopics._3()[i]);
			}
			Map<Integer, Double> articles = new HashMap<>();
			Tuple2<long[], double[]>[] documentsForTopic = model.topDocumentsPerTopic(howMany);
			for (Entry<Integer, Double> entry : topicWeights.entrySet()) {
				long[] documents = documentsForTopic[entry.getKey()]._1();
				double[] documentWeights = documentsForTopic[entry.getKey()]._2();
				for (int i = 0; i < Math.min(documents.length, howMany); i++) {
					articles.merge((int) documents[i], documentWeights[i] * entry.getValue(), Double::sum);
				}
			}
			return articles.entrySet().stream().sorted((t1, t2) -> Double.compare(t2.getValue(), t1.getValue()))
					.limit(howMany).map(t -> new Recommendation(t.getValue(), t.getKey())).collect(Collectors.toList());
		}
		return Collections.emptyList();
	}

	public static LDA_Recommender load(SparkContext sc, String path, FileSystem fs) {
		return new LDA_Recommender(DistributedLDAModel.load(sc, fs.makeQualified(path).toString()));
	}

	public static LDA_Recommender load(SparkContext sc, String path) {
		return new LDA_Recommender(DistributedLDAModel.load(sc, path));
	}

	public static LDA_Recommender load(JavaSparkContext jsc, String path, FileSystem fs) {
		return load(jsc.sc(), path, fs);
	}

	public static LDA_Recommender train(JavaSparkContext jsc, String data, int numTopics, int iterations,
			FileSystem fs) {
		LDA lda = new LDA().setK(numTopics).setMaxIterations(iterations).setOptimizer("em");
		JavaPairRDD<Long, Vector> documents = jsc.textFile(fs.makeQualified(data).toString())
				.mapToPair(LDA_Recommender::parseDocuments).cache();
		DistributedLDAModel model = (DistributedLDAModel) lda.run(documents);
		return new LDA_Recommender(model);
	}

	public LDA_Recommender save(String path, FileSystem fs) {
		model.save(model.topicAssignments().context(), fs.makeQualified(path).toString());
		return this;
	}

	public LDA_Recommender(DistributedLDAModel model) {
		this.model = model;
	}

	private static Tuple2<Long, Vector> parseDocuments(String s) {
		String[] split = s.split(";");
		long l = Long.parseLong(split[0]);
		Vector v = Vectors.parse(split[split.length - 1]);
		return new Tuple2<>(l, v);
	}
}