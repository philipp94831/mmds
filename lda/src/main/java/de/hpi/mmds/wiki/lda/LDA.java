package de.hpi.mmds.wiki.lda;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import de.hpi.mmds.wiki.Recommendation;
import de.hpi.mmds.wiki.Recommender;

public class LDA implements Recommender {

	@Override
	public List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany) {
		// TODO Auto-generated method stub
		return null;
	}

}
