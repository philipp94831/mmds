package de.hpi.mmds.parsing.edits;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.github.philipp94831.stax2parser.Stax2Parser;

import de.hpi.mmds.wiki.Spark;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import scala.Tuple2;

import javax.xml.stream.XMLStreamException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

public class EditParser {

	private static final Logger LOGGER = Logger.getLogger(EditParser.class.getName());
	@Parameter(names = "--help", help = true)
	private boolean help = false;
	@Parameter(names = "-input", description = "Input data dir", required = true)
	private String INPUT_DIR;
	@Parameter(names = "-output", description = "Output data dir", required = true)
	private String OUTPUT_DIR;
	@Parameter(names = "-version", description = "Version of Wikipedia data to use")
	private String version = "20160407";
	@Parameter(names = "-split", description = "Date where to split test and training data in the format dd.MM.yyyy")
	private String dateThreshold = "01.01.2012";
	private static final String URL = "https://dumps.wikimedia.org/enwiki/";

	public static void main(String[] args) throws IOException, ParseException {
		EditParser parser = new EditParser();
		JCommander jc = new JCommander(parser, args);
		if (parser.help) {
			jc.usage();
			System.exit(0);
		}
		parser.run();
	}

	private void run() throws IOException, ParseException {
		long start = System.nanoTime();
		parseRaw();
		Date threshold = new SimpleDateFormat("dd.MM.yyyy").parse(dateThreshold);
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
		try (JavaSparkContext jsc = Spark.newApp(EditParser.class.getName()).setMaster("local[4]").setWorkerMemory("2g")
				.context()) {
			File[] files = new File(getTempDir()).listFiles();
			for (File file : files) {
				if (file.isFile()) {
					JavaRDD<Revision> revisions = jsc.textFile(file.getPath()).map(EditParser::parseRevision)
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
		long time = System.nanoTime() - start;
		System.out.println("Total time: " + time / 1_000_000 + "ms");
	}

	private void parseRaw() {
		String tmpDir = getTempDir();
		try {
			Document raw = Jsoup.connect(URL + version + "/").get();
			Elements elements = raw.select("body > ul > li:nth-child(10) > ul > li.file > a");
			new File(tmpDir).mkdir();
			EditWriter out = new EditWriter(tmpDir + "data", 51, 51_000_000L);
			EditHandler handler = new EditHandler(out);
			Stax2Parser parser = new Stax2Parser(handler);
			List<Element> files = new ArrayList<>();
			for (Element element : elements) {
				String name = element.ownText();
				if (name.startsWith("enwiki-" + version + "-stub-meta-history") && name.endsWith(".xml.gz")) {
					files.add(element);
				}
			}
			int i = 1;
			for (Element element : files) {
				String name = element.ownText();
				LOGGER.info("Parsing file " + i + "/" + files.size() + ": " + name);
				File file = new File(INPUT_DIR + name);
				if (!file.exists()) {
					String _url = element.attr("href");
					java.net.URL url = new URL("https://dumps.wikimedia.org" + _url);
					FileUtils.copyURLToFile(url, file);
				}
				InputStream in = new GZIPInputStream(new FileInputStream(file));
				parser.parse(in);
				i++;
			}
			handler.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (XMLStreamException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private String getTempDir() {
		return INPUT_DIR + "/tmp/";
	}

	private static void aggregate(String fname, JavaRDD<Revision> revisions) {
		Iterator<String> text = revisions.mapToPair(
				t -> new Tuple2<>(new Tuple2<>(t.getUserId(), t.getArticleId()), new Tuple2<>(1, t.getTextLength())))
				.reduceByKey((t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2))
				.map(v1 -> v1._1._1 + "," + v1._1._2 + "," + v1._2._1 + "," + v1._2._2).toLocalIterator();
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
