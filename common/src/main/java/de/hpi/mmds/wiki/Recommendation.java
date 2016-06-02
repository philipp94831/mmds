package de.hpi.mmds.wiki;


public class Recommendation {
	
	private final double prediction;
	private final int article;
	public Recommendation(double prediction, int article) {
		this.prediction = prediction;
		this.article = article;
	}
	
	public double getPrediction() {
		return prediction;
	}
	
	public int getArticle() {
		return article;
	}
	
	@Override
	public String toString() {
		return article + ": " + prediction;
	}
}
