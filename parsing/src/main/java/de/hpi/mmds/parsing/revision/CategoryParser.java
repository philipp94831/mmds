package de.hpi.mmds.parsing.revision;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import de.hpi.mmds.wiki.SparkUtil;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CategoryParser {

	private static final String INPUT_DIR = "dumps/";
	private static final String OUTPUT_DIR = "data/raw/";

	private static final int STATE_OUTSIDE = 1;
	private static final int STATE_IN_ENTRY = 2;
	private static final int STATE_IN_STRING = 3;
	private static final int STATE_IN_STRING_ESCAPED = 4;

	public static void main(String[] args)
	{
		// First run
		parseCategories();
		parseCategoryLinks();

		// Second run
		canonicalizateCategories();
	}

	private static void canonicalizateCategories()
	{
		HashMap<String, Integer> categories = new HashMap<>();
		try (Stream<String> stream = Files.lines(Paths.get(OUTPUT_DIR + "category.txt/part-00000"))) {
			stream.forEach(line -> {
				List<String> parts = parseCSVLine(line);
				categories.put(parts.get(1), Integer.parseInt(parts.get(0)));
			});
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (JavaSparkContext sc = SparkUtil.getContext()) {
			JavaRDD<String> categoryLinks = sc.textFile(OUTPUT_DIR + "categorylinks.txt/part-00000");


			//
			// Pages
			//

			JavaRDD<String> pages = categoryLinks.filter(line -> {
				List<String> entry = parseCSVLine(line);
				assert entry != null;

				return entry.get(6).equals("page");
			});

			JavaRDD<String> resolvedPages = pages.map(line -> {
				List<String> entry = parseCSVLine(line);
				assert entry != null;

				return entryToCsv(Arrays.asList(entry.get(0), categories.get(entry.get(1)).toString()));
			});
			writeOutput(resolvedPages, OUTPUT_DIR + "resolved_pages.txt");


			//
			// Subcats
			//

			JavaRDD<String> subcats = categoryLinks.filter(line -> {
				List<String> entry = parseCSVLine(line);
				assert entry != null;
				return entry.get(6).equals("subcat");
			});

			JavaRDD<String> resolvedSubcats = subcats.map(line -> {
				List<String> entry = parseCSVLine(line);
				assert entry != null;

				Integer categoryId = categories.get(entry.get(1));
				if (categoryId == null) {
					return "";
				}

				return entryToCsv(Arrays.asList(entry.get(0), categoryId.toString()));
			});
			writeOutput(resolvedSubcats, OUTPUT_DIR + "resolved_subcats.txt");

		} catch (Exception e) {
			// nothing to do
		}
	}


	public static void parseCategories()
	{
		try (JavaSparkContext sc = SparkUtil.getContext()) {
			JavaRDD<String> textFile = sc.textFile(INPUT_DIR + "enwiki-20160407-category.sql");
			JavaRDD<String> filteredLines = textFile.filter(line -> line.startsWith("INSERT INTO"));
			JavaRDD<String> entries = filteredLines.flatMap(CategoryParser::parseSQLLineToCSV);

			writeOutput(entries, OUTPUT_DIR + "category.txt");

		} catch (Exception e) {
			// nothing to do
		}
	}


	private static void parseCategoryLinks()
	{
		try (JavaSparkContext sc = SparkUtil.getContext()) {
			JavaRDD<String> textFile = sc.textFile(INPUT_DIR + "enwiki-20160407-categorylinks.sql");
			JavaRDD<String> filteredLines = textFile.filter(line -> line.startsWith("INSERT INTO"));
			JavaRDD<String> entries = filteredLines.flatMap(CategoryParser::parseSQLLineToCSV);

			writeOutput(entries, OUTPUT_DIR + "categorylinks.txt");

		} catch (Exception e) {
			// nothing to do
		}
	}


	public static List<String> parseSQLLineToCSV(String line) {
		List<List<String>> entries = new ArrayList<>();
		List<String> parts = new ArrayList<>();
		String entryBuffer = "";
		int currentState = STATE_OUTSIDE;

		for (int i = 0; i < line.length(); i++) {
			char ch = line.charAt(i);

			switch (currentState) {
				case STATE_OUTSIDE:
					switch (ch) {
						case '(':
							currentState = STATE_IN_ENTRY;
							break;
					}
					break;

				case STATE_IN_ENTRY:
					switch (ch) {
						case '\'':
							currentState = STATE_IN_STRING;
							break;
						case ',':
							parts.add(entryBuffer);
							entryBuffer = "";
							break;
						case ')':
							parts.add(entryBuffer);
							entryBuffer = "";
							entries.add(parts);
							parts = new ArrayList<>();
							currentState = STATE_OUTSIDE;
							break;
						default:
							entryBuffer += ch;
					}
					break;

				case STATE_IN_STRING:
					switch (ch) {
						case '\\':
							currentState = STATE_IN_STRING_ESCAPED;
							break;
						case '\'':
							currentState = STATE_IN_ENTRY;
							break;
						default:
							entryBuffer += ch;
					}
					break;

				case STATE_IN_STRING_ESCAPED:
					entryBuffer += ch;
					currentState = STATE_IN_STRING;
					break;
			}
		}

		return entries.stream().map(CategoryParser::entryToCsv).collect(Collectors.toList());
	}


	public static List<String> parseCSVLine(String line)
	{
		List<String> parts = new ArrayList<>();
		String partBuffer = "";
		int currentState = STATE_OUTSIDE;

		for (int i = 0; i < line.length(); i++) {
			char ch = line.charAt(i);

			switch (currentState) {
				case STATE_OUTSIDE:
					switch (ch) {
						case '"':
							currentState = STATE_IN_ENTRY;
							break;
						case ',':
							break;
						default:
							partBuffer += ch;
							currentState = STATE_IN_ENTRY;
					}
					break;

				case STATE_IN_ENTRY:
					switch (ch) {
						case '"':
							parts.add(partBuffer);
							partBuffer = "";
							currentState = STATE_OUTSIDE;
							break;
						case ',':
							parts.add(partBuffer);
							partBuffer = "";
							currentState = STATE_OUTSIDE;
						default:
							partBuffer += ch;
					}
					break;
			}
		}

		return parts;
	}


	public static String entryToCsv(List<String> entry) {
		return entryToCsv(entry,  CSVWriter.NO_QUOTE_CHARACTER);
	}


	public static String entryToCsv(List<String> entry, char qm) {
		StringWriter outCSV = new StringWriter();
		try {
			CSVWriter writer = new CSVWriter(outCSV, ',', qm, "");
			writer.writeNext(entry.toArray(new String[]{}));
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return outCSV.toString();
	}


	private static void writeOutput(JavaRDD<String> data, String outFile) throws IOException {
		File output = new File(outFile);
		if (output.exists()) {
			FileUtils.forceDelete(output);
		}

		data.coalesce(1).saveAsTextFile(outFile);
	}
}
