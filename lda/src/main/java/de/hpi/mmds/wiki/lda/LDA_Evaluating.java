package de.hpi.mmds.wiki.lda;

import de.hpi.mmds.wiki.Edits;
import de.hpi.mmds.wiki.Recommender;
import de.hpi.mmds.wiki.Evaluator;
import de.hpi.mmds.wiki.Spark;
import de.hpi.mmds.wiki.FileSystem;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class LDA_Evaluating {

	public static void main(String[] args) {
		try (JavaSparkContext jsc = Spark.newApp("MMDS Wiki").context(); FileSystem fs = FileSystem.getLocal()) {
			Edits training = new Edits(jsc, "C:/Users/Marianne/Documents/Uni/HPI/Semester_1/MMDS-Mining_Massive_Datasets/data/test", fs);
			Recommender recommender = LDARecommender
					.load(jsc.sc(), "C:/Users/Marianne/Documents/Uni/HPI/Semester_1/MMDS-Mining_Massive_Datasets/data/model");
			File file = new File("C:/Users/Marianne/Documents/Uni/HPI/Semester_1/MMDS-Mining_Massive_Datasets/data/eval-output");
			try (FileOutputStream out = new FileOutputStream(file)) {
				Evaluator eval = new Evaluator(recommender, training, "C:/Users/Marianne/Documents/Uni/HPI/Semester_1/MMDS-Mining_Massive_Datasets/data/ground_truth", out, fs);
				eval.evaluate(200, 100);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}