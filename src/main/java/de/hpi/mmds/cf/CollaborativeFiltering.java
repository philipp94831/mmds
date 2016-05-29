package de.hpi.mmds.cf;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class CollaborativeFiltering {

	public static void main(String[] args) throws TasteException {
		CollaborativeFiltering cf = new CollaborativeFiltering(new File("final/training.txt"));
		List<RecommendedItem> recommendations = cf.recommend(296804, 100);
		System.out.println(recommendations);
	}
	
	private List<RecommendedItem> recommend(int userID, int howMany) throws TasteException {
		return recommender.recommend(userID, howMany);
	}

	private final Recommender recommender;
	
	
	public CollaborativeFiltering(File data) {
		try {
			DataModel model = new FileDataModel(data);
			UserSimilarity userSimilarity = new PearsonCorrelationSimilarity(model);
			UserNeighborhood neighborhood = new NearestNUserNeighborhood(3, userSimilarity, model);
			Recommender userBasedRecommender = new GenericUserBasedRecommender(model, neighborhood, userSimilarity);
			this.recommender = new CachingRecommender(userBasedRecommender);
		} catch (IOException | TasteException e) {
			throw new RuntimeException("Error building recommender", e);
		}
	}
}
