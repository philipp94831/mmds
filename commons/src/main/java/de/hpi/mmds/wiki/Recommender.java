package de.hpi.mmds.wiki;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

public interface Recommender {

	default List<Recommendation> recommend(int userId, JavaRDD<Integer> articles) {
		return recommend(userId, articles, 10);
	}

	List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany);
}
