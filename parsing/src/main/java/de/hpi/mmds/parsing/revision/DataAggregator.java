package de.hpi.mmds.parsing.revision;

import de.hpi.mmds.wiki.Spark;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DataAggregator {

	private static final String INPUT_DIR = "data/raw/";
	private final static String OUTPUT_DIR = "data/edits/";

	private static void aggregate(JavaSparkContext jsc, String dir)
			throws FileNotFoundException, IOException, ParseException {
		File d = new File(dir);
		Date threshold = new SimpleDateFormat("dd.MM.yyyy").parse("01.01.2012");
		if (!d.isDirectory()) {
			throw new IllegalArgumentException("Must pass a directory");
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
				JavaRDD<Revision> revisions = jsc.textFile(file.getPath()).map(DataAggregator::parseRevision)
						.filter(v1 -> !(bots.contains(v1.getUserId()) || v1.isMinor()));
				revisions.cache();
				JavaRDD<Revision> test = revisions.filter(v1 -> v1.getTimestamp().compareTo(threshold) >= 0);
				aggregate(OUTPUT_DIR + "test_" + file.getName(), test);
				JavaRDD<Revision> training = revisions.filter(v1 -> v1.getTimestamp().compareTo(threshold) < 0);
				aggregate(OUTPUT_DIR + "training_" + file.getName(), training);
				revisions.unpersist();
			}
		}
	}

	private static void aggregate(String fname, JavaRDD<Revision> revisions) {
		Iterator<String> text = revisions.mapToPair(
				t -> new Tuple2<>(new Tuple2<>(t.getUserId(), t.getArticleId()), new Tuple2<>(1, t.getTextLength())))
				.reduceByKey((t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2))
				.map(v1 -> v1._1._1 + "," + v1._1._2 + "," + v1._2._1 + "," +  v1._2._2).toLocalIterator();
		File outf = new File(fname);
		try {
			outf.getParentFile().mkdirs();
			if (outf.exists()) {
				FileUtils.forceDelete(outf);
			}
			try (Writer out = new BufferedWriter(new FileWriter(outf))) {
				while (text.hasNext()) {
					out.write(text.next() + "\n");
				}
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try (JavaSparkContext jsc = Spark.newApp(DataAggregator.class.getName()).setMaster("local[4]")
				.setWorkerMemory("2g").context()) {
			aggregate(jsc, INPUT_DIR);
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

	private static Revision parseRevision(String v1) throws ParseException {
		String[] split = v1.split(",");
		if (split.length != 5) {
			throw new IllegalStateException("Edits malformed");
		}
		Revision revision = new Revision(Long.parseLong(split[0]));
		revision.setUserId(Long.parseLong(split[1]));
		revision.setTextLength(split[2].equals("null") ? 1 : Integer.parseInt(split[2]));
		revision.setMinor(Boolean.parseBoolean(split[3]));
		revision.setTimestamp(split[4]);
		return revision;
	}
}
