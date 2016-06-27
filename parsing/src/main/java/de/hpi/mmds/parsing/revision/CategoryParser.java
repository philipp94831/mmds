package de.hpi.mmds.parsing.revision;

import de.hpi.mmds.wiki.Spark;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

public class CategoryParser {

	private static final String SPARK_CONTEXT_NAME = "MMDS Wiki";
	private static final String INPUT_DIR = "dumps/";
	private static final String OUTPUT_DIR = "data/raw/";

	public static void main(String[] args) {
		// First run
		parseCategories();
		parseCategoryLinks();

		// Second run
		canonicalizateCategories();
	}

	private static void canonicalizateCategories() {
		HashMap<String, Integer> categories = new HashMap<>();
		try (Stream<String> stream = Files.lines(Paths.get(OUTPUT_DIR + "category.txt/part-00000"))) {
			stream.forEach(line -> {
				List<String> parts = Csv.readLn(line);
				categories.put(parts.get(1), Integer.parseInt(parts.get(0)));
			});
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (JavaSparkContext sc = Spark.newApp(SPARK_CONTEXT_NAME).context()) {
			JavaRDD<String> categoryLinks = sc.textFile(OUTPUT_DIR + "categorylinks.txt/part-00000");

			//
			// Pages
			//

			JavaRDD<String> pages = categoryLinks.filter(line -> {
				List<String> entry = Csv.readLn(line);
				assert entry != null;

				return entry.get(6).equals("page");
			});

			JavaRDD<String> resolvedPages = pages.map(line -> {
				List<String> entry = Csv.readLn(line);
				assert entry != null;

				return Csv.writeLn(Arrays.asList(entry.get(0), categories.get(entry.get(1)).toString()));
			});
			writeOutput(resolvedPages, OUTPUT_DIR + "resolved_pages.txt");

			//
			// Subcats
			//

			JavaRDD<String> subcats = categoryLinks.filter(line -> {
				List<String> entry = Csv.readLn(line);
				assert entry != null;
				return entry.get(6).equals("subcat");
			});

			JavaRDD<String> resolvedSubcats = subcats.map(line -> {
				List<String> entry = Csv.readLn(line);
				assert entry != null;

				return Csv.writeLn(Arrays.asList(entry.get(0), categories.get(entry.get(1)).toString()));
			});
			writeOutput(resolvedSubcats, OUTPUT_DIR + "resolved_subcats.txt");

		} catch (Exception e) {
			// nothing to do
		}
	}

	public static void parseCategories() {
		try (JavaSparkContext sc = Spark.newApp(SPARK_CONTEXT_NAME).context()) {
			JavaRDD<String> textFile = sc.textFile(INPUT_DIR + "enwiki-20160407-category.sql");
			JavaRDD<String> filteredLines = textFile.filter(line -> line.startsWith("INSERT INTO"));
			JavaRDD<String> entries = filteredLines.flatMap(Csv::readSqlLn);

			writeOutput(entries, OUTPUT_DIR + "category.txt");

		} catch (Exception e) {
			// nothing to do
		}
	}

	private static void parseCategoryLinks() {
		try (JavaSparkContext sc = Spark.newApp(SPARK_CONTEXT_NAME).context()) {
			JavaRDD<String> textFile = sc.textFile(INPUT_DIR + "enwiki-20160407-categorylinks.sql");
			JavaRDD<String> filteredLines = textFile.filter(line -> line.startsWith("INSERT INTO"));
			JavaRDD<String> entries = filteredLines.flatMap(Csv::readSqlLn);

			writeOutput(entries, OUTPUT_DIR + "categorylinks.txt");

		} catch (Exception e) {
			// nothing to do
		}
	}

	private static void writeOutput(JavaRDD<String> data, String outFile) throws IOException {
		File output = new File(outFile);
		if (output.exists()) {
			FileUtils.forceDelete(output);
		}

		data.coalesce(1).saveAsTextFile(outFile);
	}
}
