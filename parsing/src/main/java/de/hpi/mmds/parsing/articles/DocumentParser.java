package de.hpi.mmds.parsing.articles;

import de.hpi.mmds.parsing.revision.DataAggregator;
import de.hpi.mmds.wiki.Edits;
import de.hpi.mmds.wiki.FileSystem;
import de.hpi.mmds.wiki.Spark;

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
				BufferedWriter out = new BufferedWriter(new FileWriter("data/2012full_articles1M.csv"));
				FileSystem fs = FileSystem.getLocal()) {
			Edits edits = new Edits(jsc, "data/edits/training_data0.txt", fs);
			Iterator<String> it = jsc.textFile("data/2012full_articles.csv")
					.mapToPair(s -> new Tuple2<>(Integer.parseInt(s.split(";")[0]), s))
					.join(edits.getArticles().mapToPair(i -> new Tuple2<>(i, null))).map(t -> t._2._1)
					.toLocalIterator();
			while (it.hasNext()) {
				out.write(it.next());
				out.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
