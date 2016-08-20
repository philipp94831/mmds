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

public class CollaborativeFiltering implements Recommender {

	private static final String PRODUCT_PATH = "/product";
	private static final String USER_PATH = "/user";
	private static final String META_PATH = "/meta";
	private static final double LOG2 = Math.log(2);
	private static final double RECOMMEND_THRESHOLD = 0.0;
	private final MatrixFactorizationModel model;
	private static final double midpoint = 1;
	private static final double steepness = 4;
	private static final double offset = logistic(0, midpoint, steepness, 1.0);

	public CollaborativeFiltering(MatrixFactorizationModel model) {
		this.model = model;
	}

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

	private static Rating parseRating(String s) {
		String[] sarray = s.split(",");
		return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
				Math.log(Double.parseDouble(sarray[2])) / LOG2 + 1);
	}

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

	private static double transformRating(double rating) {
		return logistic(rating, midpoint, steepness, 1.0 + offset) - offset;
	}

	private static double logistic(double x, double midpoint, double steepness, double max) {
		return max / (1 + Math.exp(-steepness * (x - midpoint)));
	}

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
