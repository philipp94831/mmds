package de.hpi.mmds.wiki;

import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

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
		Scanner in = new Scanner(System.in);
		System.out.println("Quit with \\q");
		while(true) {
			System.out.println("Enter user id: ");
			String input = in.next();
			if(input.equals("\\q")) {
				return;
			}
			try {
				int user = Integer.parseInt(input);
				List<Recommendation> result = recommender.recommend(user, data.getEdits(user), howMany);
				System.out.println("Recommendations for user " +  user + ":");
				System.out.println(result.stream().map(Recommendation::getArticle).collect(Collectors.toList()));
			} catch (NumberFormatException e) {
				System.out.println("Invalid user id");
			}
		}
	}

}
