package de.hpi.mmds.parsing;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.github.philipp94831.stax2parser.Stax2Parser;

public class DumpParser {

	private static final Logger LOGGER = Logger.getLogger(DumpParser.class.getName());

	public static void main(String[] args) {
		long start = System.nanoTime();
		try {
			Document raw = Jsoup.connect("https://dumps.wikimedia.org/enwiki/20160407/").get();
			Elements elements = raw.select("body > ul > li:nth-child(10) > ul > li.file > a");
			new File("data/raw/").mkdir();
			DumpWriter out = new DumpWriter("data/raw/data", 51, 51_000_000L);
			DumpHandler handler = new DumpHandler(out);
			Stax2Parser parser = new Stax2Parser(handler);
			List<Element> files = new ArrayList<>();
			for (Element element : elements) {
				String name = element.ownText();
				if (name.startsWith("enwiki-20160407-stub-meta-history") && name.endsWith(".xml.gz")) {
					files.add(element);
				}
			}
			int i = 1;
			for (Element element : files) {
				String name = element.ownText();
				LOGGER.info("Parsing file " + i + "/" + files.size() + ": " + name);
				File file = new File("dumps/" + name);
				if (!file.exists()) {
					String _url = element.attr("href");
					URL url = new URL("https://dumps.wikimedia.org" + _url);
					FileUtils.copyURLToFile(url, file);
				}
				InputStream in = new GZIPInputStream(new FileInputStream(file));
				parser.parse(in);
				i++;
			}
			handler.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (XMLStreamException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long time = System.nanoTime() - start;
		System.out.println("Total time: " + time / 1_000_000 + "ms");
	}

	public static String readWebPage(String _url) throws IOException {
		URL url = new URL(_url);
		InputStream is = url.openStream();
		int ptr = 0;
		StringBuffer buffer = new StringBuffer();
		while ((ptr = is.read()) != -1) {
			buffer.append((char) ptr);
		}
		return buffer.toString();
	}
}
