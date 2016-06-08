package de.hpi.mmds.wiki.categories;

import de.hpi.mmds.wiki.Recommendation;
import de.hpi.mmds.wiki.Recommender;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

public class CategoryAnalyzer implements Recommender {

	@Override
	public List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany) {
		// TODO Auto-generated method stub
		return null;
	}

}
