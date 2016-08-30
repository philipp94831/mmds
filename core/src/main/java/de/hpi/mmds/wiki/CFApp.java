package de.hpi.mmds.wiki;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import de.hpi.mmds.wiki.cf.CollaborativeFiltering;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

public class CFApp {

	@Parameter(names = "-fs", description = "File system to use. May be either an HDFS URL or local", required = true)
	private String uri = "local";
	@Parameter(names = "--help", help = true)
	private boolean help = false;
	@Parameter(names = "-how-many", description = "Number of recommendations to make")
	private int howMany = 10;
	@Parameter(names = "-data", description = "Path to historical data to make recommendations", required = true)
	private String dataDir;

	@Parameter(names = "-path", description = "Path to CF model", required = true)
	private String cfDir;

	public static void main(String[] args) {
		CFApp app = new CFApp();
		JCommander jc = new JCommander(app, args);
		if(app.help) {
			jc.usage();
			System.exit(0);
		}
		app.run();
	}

	public void run() {
		try (JavaSparkContext jsc = Spark.newApp("MMDS Wiki").setMaster("local[4]").setWorkerMemory("2g").context();
				FileSystem fs = FileSystem.get(uri)) {
			Recommender recommender = CollaborativeFiltering.load(jsc, cfDir, fs);
			Edits data = new Edits(jsc, dataDir, fs);
			new ConsoleRecommenderDemo(recommender, data, howMany).run();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
