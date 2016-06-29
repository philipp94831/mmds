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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LDA_Recommender implements Serializable, Recommender {

	private static final int TOP_TOPICS = 3;
	private final transient DistributedLDAModel model;
	private static final long serialVersionUID = 5290472017062948753L;
	private final transient JavaPairRDD<Long, Tuple2<int[], double[]>> topicsPerDocument;
	private transient Tuple2<long[], double[]>[] topDocumentsPerTopic;

	@Override
	public List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany) {
		return recommend(articles, howMany);
	}

	public List<Recommendation> recommend(JavaRDD<Integer> articles, int howMany) {
		JavaPairRDD<Integer, Double> topics = topicsPerDocument
				.join(articles.mapToPair(i -> new Tuple2<>((long) i, null))).map(t -> t._2._1).flatMapToPair(t -> {
					List<Tuple2<Integer, Double>> l = new ArrayList<>();
					int[] topicIds = t._1();
					double[] weights = t._2();
					for (int i = 0; i < topicIds.length; i++) {
						l.add(new Tuple2<>(topicIds[i], weights[i]));
					}
					return l;
				}).reduceByKey(Double::sum);
		Iterator<Tuple2<Integer, Double>> it = topics.toLocalIterator();
		Map<Integer, Double> recommendations = new HashMap<>();
		Tuple2<long[], double[]>[] documentsForTopic = getTopDocumentsPerTopic(howMany);
		while (it.hasNext()) {
			Tuple2<Integer, Double> topic = it.next();
			long[] documents = documentsForTopic[topic._1]._1;
			double[] documentWeights = documentsForTopic[topic._1]._2;
			for (int i = 0; i < Math.min(documents.length, howMany); i++) {
				recommendations.merge((int) documents[i], documentWeights[i] * topic._2, Double::sum);
			}
		}
		return recommendations.entrySet().stream().sorted((t1, t2) -> Double.compare(t2.getValue(), t1.getValue()))
				.limit(howMany).map(t -> new Recommendation(t.getValue(), t.getKey())).collect(Collectors.toList());
	}

	private Tuple2<long[], double[]>[] getTopDocumentsPerTopic(int howMany) {
		if (topDocumentsPerTopic == null) {
			topDocumentsPerTopic = model.topDocumentsPerTopic(TOP_TOPICS * howMany);
		}
		return topDocumentsPerTopic;
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
				.mapToPair(LDA_Recommender::parseDocuments);
		DistributedLDAModel model = (DistributedLDAModel) lda.run(documents);
		return new LDA_Recommender(model);
	}

	public LDA_Recommender save(String path, FileSystem fs) {
		model.save(model.topicAssignments().context(), fs.makeQualified(path).toString());
		return this;
	}

	public LDA_Recommender(DistributedLDAModel model) {
		this.model = model;
		this.topicsPerDocument = model.javaTopTopicsPerDocument(TOP_TOPICS)
				.mapToPair(t -> new Tuple2<>(t._1(), new Tuple2<>(t._2(), t._3()))).cache();
	}

	private static Tuple2<Long, Vector> parseDocuments(String s) {
		String[] split = s.split(";");
		long l = Long.parseLong(split[0]);
		Vector v = Vectors.parse(split[split.length - 1]);
		return new Tuple2<>(l, v);
	}
}