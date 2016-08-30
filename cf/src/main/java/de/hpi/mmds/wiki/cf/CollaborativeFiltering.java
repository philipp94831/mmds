package de.hpi.mmds.wiki.cf;

import de.hpi.mmds.wiki.FileSystem;
import de.hpi.mmds.wiki.Recommendation;
import de.hpi.mmds.wiki.Recommender;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * A recommender which uses Collaborative Filtering as model. The model can be trained, saved and loaded using this
 * class and finally recommendations can be made.
 */
public class CollaborativeFiltering implements Recommender {

	private static final String PRODUCT_PATH = "/product";
	private static final String USER_PATH = "/user";
	private static final String META_PATH = "/meta";
	private static final double LOG2 = Math.log(2);
	private static final double RECOMMEND_THRESHOLD = 0.0;
	private final MatrixFactorizationModel model;

	public CollaborativeFiltering(MatrixFactorizationModel model) {
		this.model = model;
	}

	/**
	 * Load a Collaborative Filtering model from file
	 *
	 * @param jsc
	 * 		JavaSparkContext to use
	 * @param filterDir
	 * 		Directory containing the model
	 * @param fs
	 * 		File system to use
	 *
	 * @return Recommender based on Collaborative Filtering
	 */
	public static CollaborativeFiltering load(JavaSparkContext jsc, String filterDir, FileSystem fs) {
		final int rank;
		try (BufferedReader in = fs.read(filterDir + META_PATH)) {
			rank = Integer.parseInt(in.readLine());
		} catch (Exception e) {
			throw new RuntimeException("Error reading metadata", e);
		}
		final JavaRDD<Tuple2<Object, double[]>> userFeatures = jsc.<Tuple2<Object, double[]>>objectFile(
				fs.makeQualified(filterDir + USER_PATH).toString()).persist(StorageLevel.MEMORY_AND_DISK());
		final JavaRDD<Tuple2<Object, double[]>> productFeatures = jsc.<Tuple2<Object, double[]>>objectFile(
				fs.makeQualified(filterDir + PRODUCT_PATH).toString()).persist(StorageLevel.MEMORY_AND_DISK());
		MatrixFactorizationModel model = new MatrixFactorizationModel(rank, userFeatures.rdd(), productFeatures.rdd());
		return new CollaborativeFiltering(model);
	}

	/**
	 * Transform data about a users edits on an article into a rating. Data has the form "user id, article id, bytes
	 * changed"
	 *
	 * @param s
	 * 		String representation of the edits
	 *
	 * @return Rating with the user id, article id and rating value
	 */
	private static Rating parseRating(String s) {
		String[] sarray = s.split(",");
		return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
				Math.log(Double.parseDouble(sarray[2])) / LOG2 + 1);
	}

	/**
	 * Train a Collaborative Filtering model.
	 *
	 * @param jsc
	 * 		JavaSparkContext
	 * @param path
	 * 		Path to CSV represented data of edits. Data has the form "user id, article id, bytes changed"
	 * @param rank
	 * 		Rank of the matrix to be used
	 * @param iterations
	 * 		Number of iteration to use for computation
	 * @param lambda
	 * 		Regularization parameter
	 * @param alpha
	 * 		Weighting parameter for ratings
	 * @param fs
	 * 		File system to use
	 *
	 * @return
	 */
	public static CollaborativeFiltering train(JavaSparkContext jsc, String path, int rank, int iterations,
			double lambda, double alpha, FileSystem fs) {
		JavaRDD<Rating> ratings = jsc.textFile(fs.makeQualified(path).toString())
				.map(CollaborativeFiltering::parseRating).persist(StorageLevel.MEMORY_AND_DISK());
		MatrixFactorizationModel model = ALS.trainImplicit(ratings.rdd(), rank, iterations, lambda, alpha);
		ratings.unpersist();
		return new CollaborativeFiltering(model);
	}

	@Override
	public List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany) {
		return recommend(userId, howMany);
	}

	/**
	 * Recommend articles for a user. The history of a user is stored in the model.
	 * Recommendations should be sorted so that the strongest recommendation is the first element in the list.
	 *
	 * @param userId
	 * 		unique id of the user
	 * @param howMany
	 * 		number of recommendations to be returned
	 *
	 * @return Sorted list of recommendations for the specified user.
	 *
	 * @see Recommendation
	 */
	public List<Recommendation> recommend(int userId, int howMany) {
		try {
			Rating[] recommendations = model.recommendProducts(userId, howMany);
			return Arrays.stream(recommendations).parallel().map(r -> new Recommendation(r.rating(), r.product()))
					.filter(r -> r.getPrediction() >= RECOMMEND_THRESHOLD).collect(Collectors.toList());
		} catch (NoSuchElementException e) {
			// user not included in the model
		}
		return Collections.emptyList();
	}

	/**
	 * Save a model to a file.
	 *
	 * @param filterDir
	 * 		Directory to save the model to
	 * @param fs
	 * 		File system to use
	 *
	 * @return This to enable cascading
	 *
	 * @throws IOException
	 */
	public CollaborativeFiltering save(String filterDir, FileSystem fs) throws IOException {
		fs.delete(filterDir);
		try (BufferedWriter out = fs.create(filterDir + META_PATH)) {
			out.write(Integer.toString(model.rank()));
			out.newLine();
		}
		model.userFeatures().saveAsObjectFile(fs.makeQualified(filterDir + USER_PATH).toString());
		model.productFeatures().saveAsObjectFile(fs.makeQualified(filterDir + PRODUCT_PATH).toString());
		return this;
	}
}
