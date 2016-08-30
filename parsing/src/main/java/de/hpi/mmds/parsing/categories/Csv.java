package de.hpi.mmds.parsing.categories;

import au.com.bytecode.opencsv.CSVWriter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Csv {

	private static final int STATE_OUTSIDE = 1;
	private static final int STATE_IN_ENTRY = 2;
	private static final int STATE_IN_STRING = 3;
	private static final int STATE_IN_STRING_ESCAPED = 4;


	public static List<String> readLn(String line) {
		List<String> parts = new ArrayList<>();
		String partBuffer = "";
		int currentState = STATE_OUTSIDE;

		for (int i = 0; i < line.length(); i++) {
			char ch = line.charAt(i);

			switch (currentState) {
				case STATE_OUTSIDE:
					switch (ch) {
						case '"':
							currentState = STATE_IN_STRING;
							break;
						case ',':
							break;
						default:
							partBuffer += ch;
							currentState = STATE_IN_ENTRY;
					}
					break;

				case STATE_IN_STRING:
					switch (ch) {
						case '"':
							currentState = STATE_IN_STRING_ESCAPED;
							break;
						default:
							partBuffer += ch;
					}
					break;

				case STATE_IN_ENTRY:
					switch (ch) {
						case ',':
							parts.add(partBuffer);
							partBuffer = "";
							currentState = STATE_OUTSIDE;
						default:
							partBuffer += ch;
					}
					break;

				case STATE_IN_STRING_ESCAPED:
					switch (ch) {
						case '"':
							partBuffer += ch;
							currentState = STATE_IN_STRING;
							break;
						default:
							parts.add(partBuffer);
							partBuffer = "";
							currentState = STATE_OUTSIDE;
					}
			}
		}

		if (!partBuffer.isEmpty()) {
			parts.add(partBuffer);
		}

		return parts;
	}


	public static String writeLn(List<String> entry) {
		return writeLn(entry, CSVWriter.NO_QUOTE_CHARACTER);
	}


	public static String writeLn(List<String> entry, char qm) {
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


	public static List<String> readSqlLn(String line) {
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

		return entries.stream().map(Csv::writeLn).collect(Collectors.toList());
	}
}
