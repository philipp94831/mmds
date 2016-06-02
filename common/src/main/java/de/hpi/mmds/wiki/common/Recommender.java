package de.hpi.mmds.wiki.common;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;

public interface Recommender {
	
	default List<Recommendation> recommend(int userId, JavaRDD<Long> articles) {
		return recommend(userId, articles, 10);
	}

	List<Recommendation> recommend(int userId, JavaRDD<Long> articles, int howMany);
}
