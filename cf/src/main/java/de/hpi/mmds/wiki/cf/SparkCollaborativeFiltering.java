package de.hpi.mmds.wiki.cf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.slf4j.Logger;

import de.hpi.mmds.wiki.common.Recommendation;
import de.hpi.mmds.wiki.common.Recommender;
import de.hpi.mmds.wiki.common.SparkUtil;
import scala.Tuple2;

@SuppressWarnings("unused")
public class SparkCollaborativeFiltering implements Serializable, Recommender {

	private static final long serialVersionUID = 5290472017062948755L;
	private static final int RANK = 35;
	private static final int NUM_ITERATIONS = 10;
	private final MatrixFactorizationModel model;
	private final Logger logger;
	private static final String FILTER_DIR = "filter/" + RANK;
	private static final String TEST_DATA = "data/final/test*.txt";
	private static final String TRAINING_DATA = "data/final/training*.txt";
	private static final String EVAL_FILE = "log/eval" + RANK + ".txt";
	private static final double LOG2 = Math.log(2);
	private static final double RECOMMEND_THRESHOLD = 0;
	private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");

	public SparkCollaborativeFiltering(JavaSparkContext jsc, String filterDir, String path) {
		logger = jsc.sc().log();
		JavaRDD<String> data = jsc.textFile(path);
		JavaRDD<Rating> ratings = data.map(new Function<String, Rating>() {

			private static final long serialVersionUID = -2591217342368981486L;

			public Rating call(String s) {
				String[] sarray = s.split(",");
				double r = Double.parseDouble(sarray[2]);
				return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
						Math.log(Double.parseDouble(sarray[2])) / LOG2 + 1);
			}
		});
		logger.info("Data imported");
		model = ALS.trainImplicit(JavaRDD.toRDD(ratings), RANK, NUM_ITERATIONS);
		logger.info("Model trained");
		try {
			FileUtils.deleteDirectory(new File(filterDir));
			model.save(jsc.sc(), filterDir);
			logger.info("Model saved");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public SparkCollaborativeFiltering(JavaSparkContext jsc, String filterDir) {
		logger = jsc.sc().log();
		this.model = MatrixFactorizationModel.load(jsc.sc(), filterDir);
		logger.info("Model loaded");
	}

	public JavaPairRDD<Integer, Iterable<Integer>> getUserData(JavaSparkContext jsc, String path) {
		JavaRDD<String> data = jsc.textFile(path);
		JavaPairRDD<Integer, Iterable<Integer>> res = data.mapToPair(new PairFunction<String, Integer, Integer>() {

			private static final long serialVersionUID = -4781040078296911266L;

			@Override
			public Tuple2<Integer, Integer> call(String s) throws Exception {
				String[] sarray = s.split(",");
				return new Tuple2<>(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]));
			}
		}).groupByKey();
		logger.info("User data loaded");
		return res;
	}

	public static void main(String[] args) {
		try (JavaSparkContext jsc = SparkUtil.getContext()) {
			SparkCollaborativeFiltering cf = new SparkCollaborativeFiltering(jsc, FILTER_DIR, TRAINING_DATA);
			cf.evaluate(jsc, 200);
			// readConsole(cf);
		} catch(Exception e) {
			
		}
	}

	

	private static void readConsole(SparkCollaborativeFiltering cf) {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String s;
		final String q = "Enter user id: ";
		System.out.print(q);
		try {
			while (!(s = br.readLine()).equals("\\q")) {
				try {
					int user = Integer.parseInt(s);
					List<Recommendation> recommendations = cf.recommend(user, 10);
					System.out.println(recommendations);
				} catch (NumberFormatException e) {
					System.err.println("Invalid Format!");
				}
				System.out.print(q);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void evaluate(JavaSparkContext jsc, int num) {
		JavaPairRDD<Integer, Iterable<Integer>> actual = getUserData(jsc, TEST_DATA);
		JavaPairRDD<Integer, Iterable<Integer>> previous = getUserData(jsc, TRAINING_DATA);
		List<Integer> uids = new ArrayList<>(previous.keys().intersection(actual.keys()).collect());
		int i = 0;
		double totalPrecision = 0.0;
		double totalRecall = 0.0;
		new File(EVAL_FILE).delete();
		Collections.shuffle(uids, new Random(1L));
		try (FileWriter out = new FileWriter(EVAL_FILE)) {
			for (int user : uids) {
				if (i >= num) {
					break;
				}
				List<Integer> recommendations = recommend(user, 100).stream().map(Recommendation::getArticle).collect(Collectors.toList());
				List<Iterable<Integer>> a = actual.lookup(user);
				List<Iterable<Integer>> p = previous.lookup(user);
				Set<Integer> gs = new HashSet<>();
				for (Iterable<Integer> l : a) {
					for (Integer j : l) {
						gs.add(j);
					}
				}
				Set<Integer> pr = new HashSet<>();
				for (Iterable<Integer> l : p) {
					for (Integer j : l) {
						pr.add(j);
					}
				}
				gs.removeAll(pr);
				if (!gs.isEmpty()) {
					Set<Integer> intersect = new HashSet<>(gs);
					intersect.retainAll(recommendations);
					double precision = recommendations.isEmpty() ? 0
							: (double) intersect.size() / recommendations.size();
					double recall = (double) intersect.size() / gs.size();
					totalPrecision += precision;
					totalRecall += recall;
					i++;
					out.write("User: " + user + "\n");
					out.write("Recommendations: " + recommendations + "\n");
					out.write("Gold standard: " + gs + "\n");
					out.write("Matches: " + intersect + "\n");
					out.write("Precision: " + precision + "\n");
					out.write("AVG Precision: " + totalPrecision / i + "\n");
					out.write("Recall: " + recall + "\n");
					out.write("AVG Recall: " + totalRecall / i + "\n");
					out.write("Processed: " + i + "\n");
					out.write("---\n");
					out.flush();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (i > 0) {
			System.out.println("AVG Precision: " + totalPrecision / i);
			System.out.println("AVG Recall: " + totalRecall / i);
		}
	}
	
	public List<Recommendation> recommend(int userId, int howMany) {
		try {
			Rating[] recommendations = model.recommendProducts(userId, howMany);
			return Arrays.stream(recommendations).filter(r -> r.rating() >= RECOMMEND_THRESHOLD).map(r -> new Recommendation(r.rating(), r.product()))
					.collect(Collectors.toList());
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return Collections.emptyList();
	}

	@Override
	public List<Recommendation> recommend(int userId, JavaRDD<Long> articles, int howMany) {
		return recommend(userId, howMany);
	}
}
