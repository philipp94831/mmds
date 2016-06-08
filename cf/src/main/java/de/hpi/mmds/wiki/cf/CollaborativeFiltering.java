package de.hpi.mmds.wiki.cf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.slf4j.Logger;

import scala.Tuple2;
import de.hpi.mmds.wiki.Edits;
import de.hpi.mmds.wiki.Recommendation;
import de.hpi.mmds.wiki.Recommender;
import de.hpi.mmds.wiki.SparkUtil;

@SuppressWarnings("unused")
public class CollaborativeFiltering implements Serializable, Recommender {

	private static final int RANK = 35;
	private static final double LOG2 = Math.log(2);
	private static final boolean MANUAL_SAVE_LOAD = true;
	private static final int NUM_ITERATIONS = 10;
	public static final String PRODUCT_PATH = "/product";
	private static final double RECOMMEND_THRESHOLD = 0.0;
	private static final long serialVersionUID = 5290472017062948755L;
	public static final String USER_PATH = "/user";

	private final Logger logger;
	private final MatrixFactorizationModel model;

	public CollaborativeFiltering(JavaSparkContext jsc, String filterDir) {
		logger = jsc.sc().log();
		model = loadModel(jsc, filterDir);
	}

	public CollaborativeFiltering(JavaSparkContext jsc, String filterDir, String path) {
		logger = jsc.sc().log();
		JavaRDD<String> data = jsc.textFile(path);
		JavaRDD<Rating> ratings = data.map(new Function<String, Rating>() {

			private static final long serialVersionUID = -2591217342368981486L;

			@Override
			public Rating call(String s) {
				String[] sarray = s.split(",");
				Double.parseDouble(sarray[2]);
				return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
						Math.log(Double.parseDouble(sarray[2])) / LOG2 + 1);
			}
		});
		ratings.cache();
		logger.info("Ratings imported");
		model = ALS.trainImplicit(JavaRDD.toRDD(ratings), RANK, NUM_ITERATIONS);
		logger.info("Model trained");
		try {
			saveModel(jsc, filterDir);
		} catch (IOException e) {
			throw new RuntimeException("Error saving model to disk", e);
		}
	}

	private MatrixFactorizationModel loadModel(JavaSparkContext jsc, String filterDir) {
		final MatrixFactorizationModel model;
		if (!MANUAL_SAVE_LOAD) {
			model = MatrixFactorizationModel.load(jsc.sc(), filterDir);
		} else {
			final int rank;
			try (BufferedReader in = new BufferedReader(new FileReader(new File(filterDir + "/meta")))) {
				rank = Integer.parseInt(in.readLine());
			} catch (IOException e) {
				throw new RuntimeException("Error reading metadata", e);
			}
			final JavaRDD<Tuple2<Object, double[]>> userFeatures = jsc.<Tuple2<Object, double[]>>objectFile(
					filterDir + USER_PATH).cache();
			final JavaRDD<Tuple2<Object, double[]>> productFeatures = jsc.<Tuple2<Object, double[]>>objectFile(
					filterDir + PRODUCT_PATH).cache();
			model = new MatrixFactorizationModel(rank, userFeatures.rdd(), productFeatures.rdd());
		}
		logger.info("Model loaded");
		return model;
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

	@Override
	public List<Recommendation> recommend(int userId, JavaRDD<Integer> articles, int howMany) {
		return recommend(userId, howMany);
	}

	private void saveModel(JavaSparkContext jsc, String filterDir) throws IOException {
		FileUtils.deleteDirectory(new File(filterDir));
		if (!MANUAL_SAVE_LOAD) {
			model.save(jsc.sc(), filterDir);
		} else {
			File metadata = new File(filterDir + "/meta");
			try (BufferedWriter out = new BufferedWriter(new FileWriter(metadata))) {
				out.write(model.rank());
				out.newLine();
			}
			model.userFeatures().saveAsObjectFile(filterDir + USER_PATH);
			model.productFeatures().saveAsObjectFile(filterDir + PRODUCT_PATH);
		}
		logger.info("Model saved");
	}
}
