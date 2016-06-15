package de.hpi.mmds.wiki.cf;

import de.hpi.mmds.wiki.HDFS;
import de.hpi.mmds.wiki.Recommendation;
import de.hpi.mmds.wiki.Recommender;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class CollaborativeFiltering implements Serializable, Recommender {

	private static final String PRODUCT_PATH = "/product";
	private static final String USER_PATH = "/user";
	private static final double LOG2 = Math.log(2);
	private static final boolean MANUAL_SAVE_LOAD = true;
	private static final double RECOMMEND_THRESHOLD = 0.0;
	private static final long serialVersionUID = 5290472017062948755L;
	private final MatrixFactorizationModel model;

	public CollaborativeFiltering(MatrixFactorizationModel model) {
		this.model = model;
	}

	public static CollaborativeFiltering load(JavaSparkContext jsc, String filterDir, HDFS fs) {
		final MatrixFactorizationModel model;
		if (!MANUAL_SAVE_LOAD) {
			model = MatrixFactorizationModel.load(jsc.sc(), filterDir);
		} else {
			final int rank;
			try (BufferedReader in = fs.read(new Path(filterDir + "/meta"))) {
				rank = Integer.parseInt(in.readLine());
			} catch (Exception e) {
				throw new RuntimeException("Error reading metadata", e);
			}
			final JavaRDD<Tuple2<Object, double[]>> userFeatures = jsc.<Tuple2<Object, double[]>>objectFile(
					filterDir + USER_PATH).cache();
			final JavaRDD<Tuple2<Object, double[]>> productFeatures = jsc.<Tuple2<Object, double[]>>objectFile(
					filterDir + PRODUCT_PATH).cache();
			model = new MatrixFactorizationModel(rank, userFeatures.rdd(), productFeatures.rdd());
		}
		return new CollaborativeFiltering(model);
	}

	private static Rating parseRating(String s) {
		String[] sarray = s.split(",");
		return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
				Math.log(Double.parseDouble(sarray[2])) / LOG2 + 1);
	}

	public static CollaborativeFiltering train(JavaSparkContext jsc, String path, int rank, int iterations,
			double lambda, double alpha) {
		JavaRDD<Rating> ratings = jsc.textFile(path).map(CollaborativeFiltering::parseRating);
		// ratings.cache();
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
			return Arrays.stream(recommendations).filter(r -> r.rating() >= RECOMMEND_THRESHOLD)
					.map(r -> new Recommendation(r.rating(), r.product())).collect(Collectors.toList());
		} catch (NoSuchElementException e) {
			// user not included in the model
		}
		return Collections.emptyList();
	}

	public CollaborativeFiltering save(String filterDir, HDFS fs) throws IOException {
		FileUtils.deleteDirectory(new File(filterDir));
		if (!MANUAL_SAVE_LOAD) {
			model.save(model.productFeatures().sparkContext(), filterDir);
		} else {
			try (BufferedWriter out = fs.create(new Path(filterDir + "/meta"))) {
				out.write(Integer.toString(model.rank()));
				out.newLine();
			}
			model.userFeatures().saveAsObjectFile(filterDir + USER_PATH);
			model.productFeatures().saveAsObjectFile(filterDir + PRODUCT_PATH);
		}
		return this;
	}
}
