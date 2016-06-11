package de.hpi.mmds.parsing.revision;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import de.hpi.mmds.wiki.SparkUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CategoryParser {

	private static final String INPUT_DIR = "dumps/";
	private static final String OUTPUT_DIR = "data/raw/";

	private static final int STATE_OUTSIDE = 1;
	private static final int STATE_IN_ENTRY = 2;
	private static final int STATE_IN_STRING = 3;
	private static final int STATE_IN_STRING_ESCAPED = 4;

	public static void main(String[] args) {
		parseCategories();
	}


	public static void parseCategories()
	{
		try (JavaSparkContext sc = SparkUtil.getContext()) {
			JavaRDD<String> textFile = sc.textFile(INPUT_DIR + "enwiki-20160407-category.sql");
			JavaRDD<String> filteredLines = textFile.filter(line -> line.startsWith("INSERT INTO"));
			JavaRDD<String> entries = filteredLines.flatMap(CategoryParser::parseLine);

			writeOutput(entries, OUTPUT_DIR + "category.txt");

		} catch (Exception e) {
			// nothing to do
		}
	}


	public static List<String> parseLine(String line) {
		List entries = new ArrayList();
		ArrayList parts = new ArrayList();
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
							parts = new ArrayList();
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

		return entries;
	}


	private static void writeOutput(JavaRDD<String> data, String outFile) throws IOException {
		File output = new File(outFile);
		if (output.exists()) {
			FileUtils.forceDelete(output);
		}

		data.coalesce(1).saveAsTextFile(outFile);
	}

}
