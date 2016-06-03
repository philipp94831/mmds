package de.hpi.mmds.wiki;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Data {
	
	private final JavaPairRDD<Integer, Iterable<Integer>> edits;
	
	public Data(JavaSparkContext jsc, String dataDir) {
		this.edits = parseEdits(jsc, dataDir);
	}

	private static JavaPairRDD<Integer, Iterable<Integer>> parseEdits(JavaSparkContext jsc, String dataDir) {
		JavaRDD<String> data = jsc.textFile(dataDir);
		JavaPairRDD<Integer, Iterable<Integer>> res = data.mapToPair(new PairFunction<String, Integer, Integer>() {

			private static final long serialVersionUID = -4781040078296911266L;

			@Override
			public Tuple2<Integer, Integer> call(String s) throws Exception {
				String[] sarray = s.split(",");
				return new Tuple2<>(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]));
			}
		}).groupByKey();
		jsc.sc().log().info("Edit data loaded");
		return res;
	}
	
	public List<Integer> getEdits(int user) {
		List<Iterable<Integer>> nested = edits.lookup(user);
		List<Integer> result = new ArrayList<>();
		for(Iterable<Integer> it : nested) {
			for(Integer i : it) {
				result.add(i);
			}
		}
		return result;
	}

	public JavaPairRDD<Integer, Iterable<Integer>> getAllEdits() {
		return edits;
	}
	
	public JavaRDD<Integer> getUsers() {
		return edits.keys();
	}

}
