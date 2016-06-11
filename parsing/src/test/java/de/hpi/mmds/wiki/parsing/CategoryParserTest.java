package de.hpi.mmds.wiki.parsing;

import de.hpi.mmds.parsing.revision.CategoryParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class CategoryParserTest {

	@Test
	public void testParseSimpleLine() {
		String line = "INSERT INTO (1,2,3),(a,b,c)\n";
		List entries = CategoryParser.parseLine(line);

		Assert.assertEquals(entries.toString(), "[[1, 2, 3], [a, b, c]]");
	}

	@Test
	public void testParseEscapedLine() {
		String line = "INSERT INTO ('\n(\\'a\\')\n',b,c),(d,e,f)\n";

		List entries = CategoryParser.parseLine(line);
		System.out.println(entries.toString());

		Assert.assertEquals(entries.toString(), "[[\n('a')\n, b, c], [d, e, f]]");
	}
}
