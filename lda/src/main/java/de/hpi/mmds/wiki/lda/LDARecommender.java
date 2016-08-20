package de.hpi.mmds.wiki.lda;

// import our stuff

import com.google.common.collect.MinMaxPriorityQueue;

import de.hpi.mmds.wiki.FileSystem;
import de.hpi.mmds.wiki.Recommendation;
import de.hpi.mmds.wiki.Recommender;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LocalLDAModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LDARecommender implements Recommender {

	private static final int TOP_TOPICS = 5;
	private static final String MODEL_PATH = "/model";
	private static final String DOCUMENT_PATH = "/documents";

	public int getPrfThreshold() {
		return prfThreshold;
	}

	public void setPrfThreshold(int prfThreshold) {
		this.prfThreshold = prfThreshold;
	}

	private int prfThreshold = 20;
	private final transient LocalLDAModel model;
	private final transient JavaPairRDD<Long, Tuple2<Integer, Double>> topicsPerDocument;
	private transient Map<Integer, List<Tuple2<Long, Double>>> topDocumentsPerTopic;
	private int cachedHowMany;

	@Override
	public List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany) {
		return recommend(articles, howMany);
	}

	public List<Recommendation> recommend(JavaRDD<Integer> articles, int howMany) {
		Set<Integer> history = new HashSet<>(articles.collect());
		List<Recommendation> recommendations = recommend(howMany, history);
		if (history.size() < prfThreshold) {
			Set<Integer> prfHistory = recommendations.stream().map(Recommendation::getArticle)
					.limit(prfThreshold - history.size()).collect(Collectors.toSet());
			prfHistory.addAll(history);
			recommendations = recommend(howMany, prfHistory);
		}
		recommendations.removeAll(history);
		return recommendations.stream().limit(howMany).collect(Collectors.toList());
	}

	private <R> List<Recommendation> recommend(int howMany, Set<Integer> history) {
		List<Tuple2<Integer, Double>> topics = topicsPerDocument.filter(t -> history.contains(t._1().intValue()))
				.mapToPair(Tuple2::_2).reduceByKey(Double::sum).collect();
		Map<Integer, List<Tuple2<Long, Double>>> documentsForTopic = getTopDocumentsPerTopic(3 * howMany);
		Map<Integer, Double> recommendations = new HashMap<>();
		double summedWeights = 0.0;
		for (Tuple2<Integer, Double> topic : topics) {
			double topicWeight = topic._2;
			summedWeights += topicWeight;
			int topicId = topic._1;
			List<Tuple2<Long, Double>> documents = documentsForTopic.get(topicId);
			for (Tuple2<Long, Double> d : documents) {
				int document = d._1().intValue();
				double score = d._2() * topicWeight;
				recommendations.merge(document, score, Double::sum);
			}
		}
		final double normalizer = summedWeights;
		return recommendations.entrySet().stream().sorted((t1, t2) -> Double.compare(t2.getValue(), t1.getValue()))
				.map(t -> new Recommendation(t.getValue() / normalizer, t.getKey())).collect(Collectors.toList());
	}

	private Map<Integer, List<Tuple2<Long, Double>>> getTopDocumentsPerTopic(int howMany) {
		if (topDocumentsPerTopic == null || cachedHowMany < howMany) {
			cachedHowMany = howMany;
			topDocumentsPerTopic = topicsPerDocument
					.mapToPair(t -> new Tuple2<>(t._2()._1(), new Tuple2<>(t._1(), t._2()._2()))).groupByKey()
					.mapValues(i -> {
						Comparator<Tuple2<Long, Double>> comp = (t1, t2) -> Double.compare(t2._2(), t1._2());
						MinMaxPriorityQueue<Tuple2<Long, Double>> queue = MinMaxPriorityQueue.orderedBy(comp)
								.maximumSize(howMany).create();
						for (Tuple2<Long, Double> elem : i) {
							queue.add(elem);
						}
						List<Tuple2<Long, Double>> l = new ArrayList<>(queue);
						l.sort(comp);
						return l;
					}).collectAsMap();
		}
		return topDocumentsPerTopic;
	}

	public static LDARecommender load(JavaSparkContext jsc, String path, FileSystem fs, String documentPath) {
		LocalLDAModel model = LocalLDAModel.load(jsc.sc(), fs.makeQualified(path).toString());
		JavaPairRDD<Long, Vector> documents = readDocuments(jsc, documentPath, fs);
		return new LDARecommender(fitDocuments(model, documents), model);
	}

	public static LDARecommender load(JavaSparkContext jsc, String path, FileSystem fs) {
		return new LDARecommender(
				JavaPairRDD.fromJavaRDD(jsc.objectFile(fs.makeQualified(path + DOCUMENT_PATH).toString())),
				LocalLDAModel.load(jsc.sc(), fs.makeQualified(path + MODEL_PATH).toString()));
	}

	public static LDARecommender train(JavaSparkContext jsc, String data, int numTopics, int iterations,
			FileSystem fs) {
		LDA lda = new LDA().setK(numTopics).setMaxIterations(iterations).setOptimizer("online");
		JavaPairRDD<Long, Vector> documents = readDocuments(jsc, data, fs);
		LocalLDAModel model = (LocalLDAModel) lda.run(documents);
		return new LDARecommender(fitDocuments(model, documents), model);
	}

	private static JavaPairRDD<Long, Vector> readDocuments(JavaSparkContext jsc, String data, FileSystem fs) {
		return jsc.textFile(fs.makeQualified(data).toString()).mapToPair(LDARecommender::parseDocuments)
				.persist(StorageLevel.MEMORY_AND_DISK());
	}

	public LDARecommender save(String path, FileSystem fs) {
		saveModel(path + MODEL_PATH, fs);
		saveDocuments(path + DOCUMENT_PATH, fs);
		return this;
	}

	public LDARecommender saveDocuments(String path, FileSystem fs) {
		topicsPerDocument.saveAsObjectFile(fs.makeQualified(path).toString());
		return this;
	}

	public LDARecommender saveModel(String path, FileSystem fs) {
		model.save(topicsPerDocument.context(), fs.makeQualified(path).toString());
		return this;
	}

	public LDARecommender(JavaPairRDD<Long, Tuple2<Integer, Double>> topicsPerDocument, LocalLDAModel model) {
		this.model = model;
		this.topicsPerDocument = topicsPerDocument.persist(StorageLevel.MEMORY_AND_DISK());
	}

	private static JavaPairRDD<Long, Tuple2<Integer, Double>> fitDocuments(LocalLDAModel model,
			JavaPairRDD<Long, Vector> documents) {
		return model.topicDistributions(documents).flatMapValues(LDARecommender::flatMapTopicAssignments);
	}

	private static List<Tuple2<Integer, Double>> flatMapTopicAssignments(Vector vector) {
		double[] array = vector.toArray();
		Comparator<Tuple2<Integer, Double>> comp = (t1, t2) -> Double.compare(t2._2(), t1._2());
		MinMaxPriorityQueue<Tuple2<Integer, Double>> queue = MinMaxPriorityQueue.orderedBy(comp).maximumSize(TOP_TOPICS)
				.create();
		for (int i = 0; i < array.length; i++) {
			queue.add(new Tuple2<>(i, array[i]));
		}
		List<Tuple2<Integer, Double>> l = new ArrayList<>(queue);
		l.sort(comp);
		return l;
	}

	private static Tuple2<Long, Vector> parseDocuments(String s) {
		String[] split = s.split(";");
		long l = Long.parseLong(split[0]);
		Vector v = Vectors.parse(split[split.length - 1]);
		return new Tuple2<>(l, v);
	}
}