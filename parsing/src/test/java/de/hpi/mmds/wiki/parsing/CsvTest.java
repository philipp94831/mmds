package de.hpi.mmds.wiki.parsing;

import de.hpi.mmds.parsing.categories.Csv;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class CsvTest {

	@Test
	public void testParseSQLLineToCSVSimple() {
		String line = "INSERT INTO (1,2,3),(a,b,c)\n";
		List entries = Csv.readSqlLn(line);
		Assert.assertEquals(Arrays.asList("1,2,3", "a,b,c"), entries);
	}

	@Test
	public void testParseSQLLineToCSVEscaped() {
		String line = "INSERT INTO ('\n(\\'a\\')\n',b,c),(d,e,f)\n";
		List entries = Csv.readSqlLn(line);
		Assert.assertEquals(
			Arrays.asList("\n('a')\n,b,c", "d,e,f"),
			entries
		);
	}

	@Test
	public void testEntryToCsv() {
		String[] entry = {"a,", "b,\",", "c'"};
		Assert.assertEquals("a,,b,\"\",,c'", Csv.writeLn(Arrays.asList(entry)));
	}
}
