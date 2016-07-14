package de.hpi.mmds.parsing.articles;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.function.Function;
import org.sweble.wikitext.engine.PageId;
import org.sweble.wikitext.engine.PageTitle;
import org.sweble.wikitext.engine.WtEngineImpl;
import org.sweble.wikitext.engine.config.WikiConfigImpl;
import org.sweble.wikitext.engine.nodes.EngProcessedPage;
import org.sweble.wikitext.engine.utils.DefaultConfigEnWp;
import org.sweble.wikitext.example.TextConverter;
import org.tartarus.snowball.ext.englishStemmer;

import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class AdvancedWikiTextParser implements Function<Article, Tuple2<Integer, String>> {

	private transient englishStemmer stemmer;
	private transient WikiConfigImpl config;
	private transient WtEngineImpl engine;
	private transient TextConverter conv;
	private static String ILLEGAL_FILE_CHARACTERS = Pattern.quote("\\/:*?\"<>|");

	private englishStemmer getStemmer() {
		if (stemmer == null) {
			stemmer = new englishStemmer();
		}
		return stemmer;
	}

	private WikiConfigImpl getConfig() {
		if (config == null) {
			config = DefaultConfigEnWp.generate();
		}
		return config;
	}

	private WtEngineImpl getEngine() {
		if (engine == null) {
			engine = new WtEngineImpl(getConfig());
		}
		return engine;
	}

	@Override
	public Tuple2<Integer, String> call(Article s) throws Exception {
		try {
			String rawText = Normalizer.normalize(s.getText(), Form.NFD).toLowerCase();
			PageTitle pageTitle = PageTitle.make(getConfig(), s.getTitle());
			PageId pageId = new PageId(pageTitle, -1);
			EngProcessedPage cp = getEngine().postprocess(pageId, rawText, null);
			TextConverter conv = getTextConverter();
			String parsed = (String) conv.go(cp.getPage());
			String replaced = parsed.replaceAll("\\W", " ");
			List<String> tknzed = Arrays.stream(replaced.split("\\W")).filter(str -> str.length() > 3).map(this::stem).collect(Collectors.toList());
			String trimmed = String.join(" ", tknzed);
			return new Tuple2<>(s.getId(), trimmed);
		} catch(Exception e) {
			File file = new File("failed/" + s.getTitle().replaceAll("[" + ILLEGAL_FILE_CHARACTERS + "]", "_") + ".txt");
			file.getParentFile().mkdirs();
			if(file.exists()) {
				FileUtils.forceDelete(file);
			}
			try(BufferedWriter out = new BufferedWriter(new FileWriter(file))) {
				out.write(s.getText());
			}
			return null;
		}
	}

	private TextConverter getTextConverter() {
		if (conv == null) {
			conv = new TextConverter(getConfig(), 80);
		}
		return conv;
	}

	private String stem(String s) {
		englishStemmer st = getStemmer();
		st.setCurrent(s);
		st.stem();
		return st.getCurrent();
	}
}
