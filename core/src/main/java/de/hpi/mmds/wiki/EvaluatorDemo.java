package de.hpi.mmds.wiki;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class EvaluatorDemo {

	private final File file;
	private final Recommender recommender;
	private final Edits edits;
	private final Edits test;

	public EvaluatorDemo(File file, Recommender recommender, Edits test, Edits edits) {
		this.file = file;
		this.recommender = recommender;
		this.edits = edits;
		this.test = test;
	}

	public void run() throws IOException {
		file.getParentFile().mkdirs();
		if (file.exists()) {
			FileUtils.forceDelete(file);
		}
		try (OutputStream out = new FileOutputStream(file)) {
			Evaluator eval = new Evaluator(recommender, test, edits, out);
			eval.evaluate(1000, 10, 1L);
		}
	}

}
