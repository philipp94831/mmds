package de.hpi.mmds.wiki;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

public interface Recommender {

	default List<Recommendation> recommend(int userId, JavaRDD<Integer> articles) {
		return recommend(userId, articles, 10);
	}

	/**
	 * Recommend articles for a user. The recommendations may use the articles a user edited or just his id.
	 * Recommendations should be sorted so that the strongest recommendation is the first element in the list.
	 *
	 * @param userId
	 * 		unique id of the user
	 * @param articles
	 * 		articles a user has edited
	 * @param howMany
	 * 		number of recommendations to be returned
	 *
	 * @return Sorted list of recommendations for the specified user.
	 *
	 * @see Recommendation
	 */
	List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany);
}
