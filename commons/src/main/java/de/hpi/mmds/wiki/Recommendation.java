package de.hpi.mmds.wiki;

import java.io.Serializable;

public class Recommendation implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8187050183236621716L;
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
