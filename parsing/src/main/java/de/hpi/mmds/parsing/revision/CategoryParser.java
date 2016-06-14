package de.hpi.mmds.parsing.revision;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import de.hpi.mmds.wiki.SparkUtil;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

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
//		parseCategories();
//		parseCategoryLinks();

		// Second run
		canonicalizateCategories();
	}

	private static void canonicalizateCategories() {

		HashMap<String, Integer> categories = new HashMap<>();
		CSVReader reader;

		try {
			reader = new CSVReader(new FileReader(OUTPUT_DIR + "category.txt/part-00000"));
			String[] line;
			while ((line = reader.readNext()) != null) {
				categories.put(line[1], Integer.parseInt(line[0]));
            }
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (JavaSparkContext sc = SparkUtil.getContext()) {
			JavaRDD<String> categoryLinks = sc.textFile(OUTPUT_DIR + "categorylinks.txt/part-00000");
			JavaRDD<String> resolved = categoryLinks.map(line -> {
				String[] entry = csvToEntry(line);
				assert entry != null;

				Integer categoryId = categories.get(entry[1]);
				if (categoryId == null) {
					//System.err.println("Id of category '" + entry[1] + "' is unknown, skipping.");
					return ""; // TODO: filter empty liens afterwards
				}

				if (entry.length < 7) {
					System.err.println("OUT OF BOUNDS: " + Arrays.toString(entry));
					return "";
				}

				return entryToCsv(Arrays.asList(entry[0], categoryId.toString(), entry[6]));
			});

			writeOutput(resolved, OUTPUT_DIR + "resolved.txt");

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


	public static String entryToCsv(List<String> entry) {
		return entryToCsv(entry, '"');
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

	public static String[] csvToEntry(String csvLine) {
		CSVReader reader = new CSVReader(new StringReader(csvLine));
		try {
			return reader.readNext();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}
}
