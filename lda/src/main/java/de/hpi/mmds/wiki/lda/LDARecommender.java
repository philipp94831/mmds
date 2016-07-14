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
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LDARecommender implements Serializable, Recommender {

	private static final int TOP_TOPICS = 50;
	private final transient DistributedLDAModel model;
	private static final long serialVersionUID = 5290472017062948753L;
	private final transient JavaPairRDD<Long, Tuple2<Integer, Double>> topicsPerDocument;
	private transient Tuple2<long[], double[]>[] topDocumentsPerTopic;
	private int cachedHowMany;

	@Override
	public List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany) {
		return recommend(articles, howMany);
	}

	public List<Recommendation> recommend(JavaRDD<Integer> articles, int howMany) {
		Set<Integer> history = new HashSet<>(articles.collect());
		JavaPairRDD<Integer, Double> topics = topicsPerDocument.filter(t -> history.contains(t._1().intValue()))
				.mapToPair(t -> t._2()).reduceByKey(Double::sum);
		Iterator<Tuple2<Integer, Double>> it = topics.toLocalIterator();
		Map<Integer, Double> recommendations = new HashMap<>();
		Tuple2<long[], double[]>[] documentsForTopic = getTopDocumentsPerTopic(howMany);
		double summedWeights = 0.0;
		while (it.hasNext()) {
			Tuple2<Integer, Double> topic = it.next();
			double topicWeight = topic._2;
			summedWeights += topicWeight;
			int topicId = topic._1;
			long[] documents = documentsForTopic[topicId]._1;
			double[] documentWeights = documentsForTopic[topicId]._2;
			for (int i = 0; i < Math.min(documents.length, howMany); i++) {
				int document = (int) documents[i];
				if (!history.contains(document)) {
					double score = documentWeights[i] * topicWeight;
					recommendations.merge(document, score, Double::sum);
				}
			}
		}
		double normalizer = summedWeights;
		return recommendations.entrySet().stream().sorted((t1, t2) -> Double.compare(t2.getValue(), t1.getValue()))
				.limit(howMany).map(t -> new Recommendation(t.getValue() / normalizer, t.getKey()))
				.collect(Collectors.toList());
	}

	private Tuple2<long[], double[]>[] getTopDocumentsPerTopic(int howMany) {
		if (topDocumentsPerTopic == null || cachedHowMany < howMany) {
			cachedHowMany = howMany;
			topDocumentsPerTopic = model.topDocumentsPerTopic(TOP_TOPICS * howMany);
		}
		return topDocumentsPerTopic;
	}

	public static LDARecommender load(SparkContext sc, String path, FileSystem fs) {
		return load(JavaSparkContext.fromSparkContext(sc), path, fs);
	}

	public static LDARecommender load(SparkContext sc, String path) {
		return load(JavaSparkContext.fromSparkContext(sc), path);
	}

	public static LDARecommender load(JavaSparkContext jsc, String path) {
		return new LDARecommender(DistributedLDAModel.load(jsc.sc(), path));
	}

	public static LDARecommender load(JavaSparkContext jsc, String path, FileSystem fs) {
		return load(jsc, fs.makeQualified(path).toString());
	}

	public static LDARecommender train(JavaSparkContext jsc, String data, int numTopics, int iterations,
			FileSystem fs) {
		LDA lda = new LDA().setK(numTopics).setMaxIterations(iterations).setOptimizer("em");
		JavaPairRDD<Long, Vector> documents = jsc.textFile(fs.makeQualified(data).toString())
				.mapToPair(LDARecommender::parseDocuments);
		DistributedLDAModel model = (DistributedLDAModel) lda.run(documents);
		return new LDARecommender(model);
	}

	public LDARecommender save(String path, FileSystem fs) {
		model.save(model.topicDistributions().context(), fs.makeQualified(path).toString());
		return this;
	}

	public LDARecommender(DistributedLDAModel model) {
		this.model = model;
		this.topicsPerDocument = model.javaTopTopicsPerDocument(TOP_TOPICS)
				.mapToPair(t -> new Tuple2<>(t._1(), new Tuple2<>(t._2(), t._3()))).flatMapValues(t -> {
					List<Tuple2<Integer, Double>> l = new ArrayList<>();
					int[] topicIds = t._1();
					double[] weights = t._2();
					for (int i = 0; i < topicIds.length; i++) {
						l.add(new Tuple2<>(topicIds[i], weights[i]));
					}
					return l;
				}).persist(StorageLevel.MEMORY_AND_DISK());
	}

	private static Tuple2<Long, Vector> parseDocuments(String s) {
		String[] split = s.split(";");
		long l = Long.parseLong(split[0]);
		Vector v = Vectors.parse(split[split.length - 1]);
		return new Tuple2<>(l, v);
	}
}