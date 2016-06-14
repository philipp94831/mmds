package de.hpi.mmds.parsing.revision;

import org.apache.commons.io.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.TreeMap;

class DumpWriter {

	private final TreeMap<Long, Writer> outs = new TreeMap<>();

	public DumpWriter(String fileName, int numPartitions, long max) throws IOException {
		for (int i = 0; i < numPartitions; i++) {
			File file = new File(fileName + i + ".txt");
			file.getParentFile().mkdirs();
			if (file.exists()) {
				FileUtils.forceDelete(file);
			}
			outs.put(i * max / numPartitions, new BufferedWriter(new FileWriter(file)));
		}
	}

	public void close() throws IOException {
		for (Writer writer : outs.values()) {
			writer.close();
		}
	}

	public void write(Revision revision) throws IOException {
		outs.floorEntry(revision.getArticleId()).getValue()
				.write(revision.getArticleId() + "," + revision.getUserId() + "," + revision.getTextLength() + ","
						+ revision.isMinor() + "," + revision.getTimestampAsString() + "\n");
	}
}
