package de.hpi.mmds.wiki.lda;

// import our stuff

import de.hpi.mmds.wiki.Recommendation;
import de.hpi.mmds.wiki.Recommender;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.DistributedLDAModel;

import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class LDA_Recommender implements Serializable, Recommender {

	private final transient DistributedLDAModel model;
	private static final long serialVersionUID = 5290472017062948753L;

	@Override
	public List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany) {
		return recommend(articles.first(), howMany);
	}

	public List<Recommendation> recommend(long article, int howMany) {
		JavaRDD<Tuple3<Long, int[], double[]>> topics = model.javaTopTopicsPerDocument(1)
				.filter(tuple -> tuple._1() == article);
		int topic = topics.first()._2()[0];
		Tuple2<long[], double[]> articles = model.topDocumentsPerTopic(howMany)[topic];
		List<Recommendation> recommendations = new ArrayList<Recommendation>();
		for (int i = 0; i < articles._1().length; i++) {
			recommendations.add(new Recommendation(articles._2()[i], (int) articles._1()[i]));
			System.out.println("Article: " + articles._1()[i] + " Probability: " + articles._2()[i]);
		}
		return recommendations;
	}

	public static LDA_Recommender load(SparkContext sc, String path) {
		return new LDA_Recommender(DistributedLDAModel.load(sc, path));
	}

	public LDA_Recommender(DistributedLDAModel model) {
		this.model = model;
	}
}