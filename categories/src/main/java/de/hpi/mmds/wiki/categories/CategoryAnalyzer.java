package de.hpi.mmds.wiki.categories;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import de.hpi.mmds.wiki.common.Recommendation;
import de.hpi.mmds.wiki.common.Recommender;

public class CategoryAnalyzer implements Recommender {

	@Override
	public List<Recommendation> recommend(int userId, JavaRDD<Long> articles, int howMany) {
		// TODO Auto-generated method stub
		return null;
	}

}
