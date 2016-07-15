package de.hpi.mmds.parsing.articles;

import de.hpi.mmds.parsing.revision.DataAggregator;
import de.hpi.mmds.wiki.FileSystem;
import de.hpi.mmds.wiki.Spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

public class DocumentParser {

	public static void main(String[] args) {
		try (JavaSparkContext jsc = Spark.newApp(DataAggregator.class.getName()).setMaster("local[4]")
				.setWorkerMemory("2g").context();
				BufferedWriter out = new BufferedWriter(new FileWriter("data/training_new.txt"));
				FileSystem fs = FileSystem.getLocal()) {
			JavaPairRDD<Integer, String> edits = jsc.textFile("data/edits/training*.txt")
					.mapToPair(s -> new Tuple2<>(Integer.parseInt(s.split(",")[1]), s));
			Iterator<String> it = jsc.textFile("data/advanced_articles.csv")
					.mapToPair(s -> new Tuple2<>(Integer.parseInt(s.split(";")[0]), null)).join(edits)
					.map(t -> t._2()._2()).toLocalIterator();
			while (it.hasNext()) {
				out.write(it.next());
				out.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
