package de.hpi.mmds.parsing.documents;

import org.apache.spark.api.java.function.Function;
import org.sweble.wikitext.engine.PageId;
import org.sweble.wikitext.engine.PageTitle;
import org.sweble.wikitext.engine.WtEngineImpl;
import org.sweble.wikitext.engine.config.WikiConfigImpl;
import org.sweble.wikitext.engine.nodes.EngProcessedPage;
import org.sweble.wikitext.engine.utils.DefaultConfigEnWp;
import org.sweble.wikitext.example.TextConverter;
import org.tartarus.snowball.ext.englishStemmer;

import scala.Int;
import scala.Tuple3;
import scala.Tuple4;

import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class AdvancedWikiTextParser implements Function<Tuple4<Int, Int, String, String>, Tuple3<Int, String, String>> {

	private transient englishStemmer stemmer;
	private transient WikiConfigImpl config;
	private transient WtEngineImpl engine;

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
	public Tuple3<Int, String, String> call(Tuple4<Int, Int, String, String> s) throws Exception {
		String rawText = Normalizer.normalize(s._4(), Form.NFD);
		PageTitle pageTitle = PageTitle.make(getConfig(), s._3());
		PageId pageId = new PageId(pageTitle, -1);
		EngProcessedPage cp = getEngine().postprocess(pageId, rawText, null);
		TextConverter conv = new TextConverter(getConfig(), 80);
		String parsed = (String) conv.go(cp.getPage());
		String replaced = parsed.replaceAll("\\W", " ");
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
