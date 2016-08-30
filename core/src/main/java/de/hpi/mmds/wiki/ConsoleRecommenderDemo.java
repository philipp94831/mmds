package de.hpi.mmds.wiki;

import java.util.List;

public class ConsoleRecommenderDemo {

	private final Recommender recommender;
	private final int howMany;
	private final Edits data;

	public ConsoleRecommenderDemo(Recommender recommender, Edits data, int howMany) {
		this.recommender = recommender;
		this.data = data;
		this.howMany = howMany;
	}

	public void run() {
		while(true) {
			String in = System.console().readLine("Enter user id: ");
			if(in.equals("\\q")) {
				System.exit(0);
			}
			try {
				int user = Integer.parseInt(in);
				List<Recommendation> result = recommender.recommend(user, data.getEdits(user), howMany);
				System.out.println("Recommendations for user " +  user + ":");
				System.out.println(result);
			} catch (NumberFormatException e) {
				System.out.println("Invalid user id");
			}
		}
	}

}
