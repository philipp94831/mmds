package de.hpi.mmds.wiki;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;

public interface Recommender {
	
	default List<Recommendation> recommend(int userId, JavaRDD<Integer> articles) {
		return recommend(userId, articles, 10);
	}

	List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany);
}
