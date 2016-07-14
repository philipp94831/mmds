package de.hpi.mmds.parsing.articles;

import java.io.Serializable;

public class Article implements Serializable {

	private final int id;
	private final String text;
	private final String title;
	private final int namespace;

	public Article(int id, String text, String title, int namespace) {
		this.id = id;
		this.text = text;
		this.title = title;
		this.namespace = namespace;
	}

	public String getText() {
		return text;
	}

	public String getTitle() {
		return title;
	}

	public int getNamespace() {
		return namespace;
	}

	public int getId() {

		return id;
	}

	public boolean isRedirect() {
		return text.startsWith("#REDIRECT");
	}

	public boolean isDisambugation() {
		return text.endsWith("{{disambiguation}}");
	}
}
