package de.hpi.mmds.wiki.lda;

// import our stuff

import com.google.common.collect.MinMaxPriorityQueue;

import de.hpi.mmds.wiki.Evaluator;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LDARecommender implements Recommender {

	private static final int TOP_TOPICS = 5;
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
		JavaPairRDD<Integer, Double> topics = topicsPerDocument.filter(t -> history.contains(t._1().intValue()))
				.mapToPair(Tuple2::_2).reduceByKey(Double::sum);
		Iterator<Tuple2<Integer, Double>> it = topics.toLocalIterator();
		Map<Integer, Double> recommendations = new HashMap<>();
		Map<Integer, List<Tuple2<Long, Double>>> documentsForTopic = getTopDocumentsPerTopic(howMany);
		double summedWeights = 0.0;
		while (it.hasNext()) {
			Tuple2<Integer, Double> topic = it.next();
			double topicWeight = topic._2;
			summedWeights += topicWeight;
			int topicId = topic._1;
			List<Tuple2<Long, Double>> documents = documentsForTopic.get(topicId);
			int i = 0;
			for (Tuple2<Long, Double> d : documents) {
				if (i++ >= howMany) {
					break;
				}
				int document = d._1().intValue();
				if (!history.contains(document)) {
					double score = d._2() * topicWeight;
					recommendations.merge(document, score, Double::sum);
				}
			}
		}
		double normalizer = summedWeights;
		return recommendations.entrySet().stream().sorted((t1, t2) -> Double.compare(t2.getValue(), t1.getValue()))
				.limit(howMany).map(t -> new Recommendation(t.getValue() / normalizer, t.getKey()))
				.collect(Collectors.toList());
	}

	private Map<Integer, List<Tuple2<Long, Double>>> getTopDocumentsPerTopic(int howMany) {
		if (topDocumentsPerTopic == null || cachedHowMany < howMany) {
			cachedHowMany = howMany;
			topDocumentsPerTopic = topicsPerDocument
					.mapToPair(t -> new Tuple2<>(t._2()._1(), new Tuple2<>(t._1(), t._2()._2()))).groupByKey()
					.mapValues(i -> {
						Comparator<Tuple2<Long, Double>> comp = (t1, t2) -> Double.compare(t2._2(), t1._2());
						MinMaxPriorityQueue<Tuple2<Long, Double>> queue = MinMaxPriorityQueue.orderedBy(comp)
								.maximumSize(3 * howMany).create();
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

	public static LDARecommender load(JavaSparkContext jsc, String path, FileSystem fs, String documents) {
		return new LDARecommender(LocalLDAModel.load(jsc.sc(), fs.makeQualified(path).toString()),
				readDocuments(jsc, documents, fs).filter(t -> t._1() % 100 < Evaluator.PERCENTAGE));
	}

	public static LDARecommender train(JavaSparkContext jsc, String data, int numTopics, int iterations,
			FileSystem fs) {
		LDA lda = new LDA().setK(numTopics).setMaxIterations(iterations).setOptimizer("online");
		JavaPairRDD<Long, Vector> documents = readDocuments(jsc, data, fs)
				.filter(t -> t._1() % 100 < Evaluator.PERCENTAGE).persist(StorageLevel.MEMORY_AND_DISK());
		LocalLDAModel model = (LocalLDAModel) lda.run(documents);
		return new LDARecommender(model, documents);
	}

	private static JavaPairRDD<Long, Vector> readDocuments(JavaSparkContext jsc, String data, FileSystem fs) {
		return jsc.textFile(fs.makeQualified(data).toString()).mapToPair(LDARecommender::parseDocuments);
	}

	public LDARecommender save(JavaSparkContext jsc, String path, FileSystem fs) {
		model.save(jsc.sc(), fs.makeQualified(path).toString());
		return this;
	}

	public LDARecommender(LocalLDAModel model, JavaPairRDD<Long, Vector> documents) {
		this.model = model;
		this.topicsPerDocument = model.topicDistributions(documents)
				.flatMapValues(LDARecommender::flatMapTopicAssignments).persist(StorageLevel.MEMORY_AND_DISK());
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