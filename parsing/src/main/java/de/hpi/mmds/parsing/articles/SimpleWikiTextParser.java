package de.hpi.mmds.parsing.articles;

import org.apache.spark.api.java.function.Function;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.sweble.wikitext.engine.WtEngineImpl;
import org.sweble.wikitext.engine.config.WikiConfigImpl;
import org.tartarus.snowball.ext.englishStemmer;

import scala.Int;
import scala.Tuple3;
import scala.Tuple4;

import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SimpleWikiTextParser implements Function<Tuple4<Int, Int, String, String>, Tuple3<Int, String, String>> {

	private transient englishStemmer stemmer;
	private transient WikiConfigImpl config;
	private transient WtEngineImpl engine;

	private englishStemmer getStemmer() {
		if (stemmer == null) {
			stemmer = new englishStemmer();
		}
		return stemmer;
	}

	@Override
	public Tuple3<Int, String, String> call(Tuple4<Int, Int, String, String> s) throws Exception {
		Element body  = Jsoup.parse(s._4()).body();
		body.getElementsByTag("ref").remove();
		body.getElementsByTag("style").remove();
		String rawText = Normalizer.normalize(body.text(), Form.NFD);
		String replaced = rawText
				.replaceAll("\\{\\{[^\\}]+\\}\\}", "")
				.replaceAll("\\[\\[Category:([^\\]]+)\\]\\]", "$1")
				.replaceAll("\\W", " ");
		englishStemmer st = getStemmer();
		List<String> tknzed = Arrays.stream(replaced.split("\\W")).filter(str -> str.length() > 3).map(str -> {
			st.setCurrent(str);
			st.stem();
			return st.getCurrent();
		}).collect(Collectors.toList());
		String trimmed = String.join(" ", tknzed);
		return new Tuple3<>(s._1(), s._3(), trimmed);
	}
}
