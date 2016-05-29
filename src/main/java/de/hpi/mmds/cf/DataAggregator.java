package de.hpi.mmds.cf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import de.hpi.mmds.parsing.Revision;
import scala.Tuple2;

public class DataAggregator {

	private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");

	public static void main(String[] args) {
		try (JavaSparkContext jsc = SparkUtil.getContext()) {
			aggregate(jsc, "data/raw/");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void aggregate(JavaSparkContext jsc, String dir) throws FileNotFoundException, IOException, ParseException {
		File d = new File(dir);
		Date threshold = new SimpleDateFormat("dd.MM.yyyy").parse("01.01.2012");
		if (!d.isDirectory()) {
			throw new IllegalArgumentException("Must pass a driectory");
		}
		Set<Long> bots = new HashSet<>();
		try (BufferedReader br = new BufferedReader(new FileReader("data/users.txt"))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] s = line.split(",");
				if (s[1].equals("bot")) {
					bots.add(Long.parseLong(s[0]));
				}
			}
		}
		File[] files = d.listFiles();
		for (File file : files) {
			if (file.isFile()) {
				JavaRDD<Revision> revisions = jsc.textFile(file.getPath()).map(new Function<String, Revision>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 856475920466882421L;

					@Override
					public Revision call(String v1) throws Exception {
						String[] split = v1.split(",");
						if (split.length != 5) {
							throw new IllegalStateException("Data malformed");
						}
						Revision revision = new Revision(Long.parseLong(split[0]));
						revision.setUserId(Long.parseLong(split[1]));
						revision.setTextLength(split[2].equals("null") ? 1 : Integer.parseInt(split[2]));
						revision.setMinor(Boolean.parseBoolean(split[3]));
						revision.setTimestamp(split[4]);
						return null;
					}
				}).filter(new Function<Revision, Boolean>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 260940817334437364L;

					@Override
					public Boolean call(Revision v1) throws Exception {
						return !(bots.contains(v1.getUserId()) || v1.isMinor());
					}
				});
				JavaRDD<Revision> test = revisions.filter(new Function<Revision, Boolean>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 4216755090418786585L;

					@Override
					public Boolean call(Revision v1) throws Exception {
						return DATE_FORMAT.parse(v1.getTimestamp()).compareTo(threshold) >= 0;
					}
				});
				aggregate("data/final/test_" + file.getName(), test);
				JavaRDD<Revision> training = revisions.filter(new Function<Revision, Boolean>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 4216755090418786585L;

					@Override
					public Boolean call(Revision v1) throws Exception {
						return DATE_FORMAT.parse(v1.getTimestamp()).compareTo(threshold) < 0;
					}
				});
				aggregate("data/final/training_" + file.getName(), training);
			}
		}
	}

	private static void aggregate(String fname, JavaRDD<Revision> revisions) {
		JavaPairRDD<Tuple2<Long, Long>, Integer> parsed = revisions
				.mapToPair(new PairFunction<Revision, Tuple2<Long, Long>, Integer>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 2015595100212783648L;

					@Override
					public Tuple2<Tuple2<Long, Long>, Integer> call(Revision t) throws Exception {
						Tuple2<Long, Long> tuple = new Tuple2<>(t.getArticleId(), t.getUserId());
						return new Tuple2<>(tuple, t.getTextLength());
					}
				});
		JavaPairRDD<Tuple2<Long, Long>, Integer> reduced = parsed
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 7936093501541164863L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});
		List<String> text = reduced.map(new Function<Tuple2<Tuple2<Long, Long>, Integer>, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -6264002531208218613L;

			@Override
			public String call(Tuple2<Tuple2<Long, Long>, Integer> v1) throws Exception {
				return v1._1._2 + "," + v1._1._1 + "," + v1._2;
			}
		}).collect();
		File outf = new File(fname);
		try {
			FileUtils.forceDelete(outf);
			try (FileWriter out = new FileWriter(outf)) {
				for (String line : text) {
					out.write(line + "\n");
				}
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
}
