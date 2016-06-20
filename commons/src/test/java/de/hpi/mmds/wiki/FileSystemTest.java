package de.hpi.mmds.wiki;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * <p>
 * <b>IMPORTANT:</b> In order to get the MiniDFCluster to work on Windows, you need to have hadoop including
 * winutils.exe in your PATH
 * </p>
 * <p>
 * Download available <a href="http://static.barik.net/software/hadoop-2.6.0-dist/hadoop-2.6.0.tar.gz">here</a>
 * </p>
 */
public class FileSystemTest {

	private static MiniDFSCluster hdfsCluster;

	@BeforeClass
	public static void setupClass() throws IOException {
		Configuration conf = new Configuration();
		File baseDir = new File("target/hdfs/").getAbsoluteFile();
		if (baseDir.exists()) {
			FileUtils.forceDelete(baseDir);
		}
		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		hdfsCluster = builder.build();
	}

	@AfterClass
	public static void tearDownClass() throws IOException {
		hdfsCluster.shutdown();
	}

	private void readWriteFile(FileSystem fs) throws IOException {
		String file = "foo/bar/test.txt";
		if (fs.exists(file)) {
			fs.delete(file);
		}
		assertFalse(fs.exists(file));
		try (BufferedWriter out = fs.create(file)) {
			out.write("hello");
		}
		assertTrue(fs.exists(file));
		try (BufferedReader in = fs.read(file)) {
			int i = 0;
			for (String line = in.readLine(); line != null; line = in.readLine(), i++) {
				assertEquals(5, line.length());
				assertEquals("hello", line);
			}
			assertEquals(1, i);
		}
		String dir = "foo";
		if (fs.exists(dir)) {
			fs.delete(dir);
		}
		assertFalse(fs.exists(file));
	}

	private void readWriteQualifiedFile(FileSystem fs) throws IOException {
		Path file = fs.makeQualified("foo/bar/test.txt");
		if (fs.exists(file)) {
			fs.delete(file);
		}
		assertFalse(fs.exists(file));
		try (BufferedWriter out = fs.create(file)) {
			out.write("hello");
		}
		assertTrue(fs.exists(file));
		try (BufferedReader in = fs.read(file)) {
			int i = 0;
			for (String line = in.readLine(); line != null; line = in.readLine(), i++) {
				assertEquals(5, line.length());
				assertEquals("hello", line);
			}
			assertEquals(1, i);
		}
		String dir = "foo";
		if (fs.exists(dir)) {
			fs.delete(dir);
		}
		assertFalse(fs.exists(file));
	}

	@Test
	public void testHDFS() throws IOException, URISyntaxException {
		try (FileSystem fs = FileSystem.get(hdfsCluster.getURI())) {
			assertEquals(hdfsCluster.getURI().toString() + "/foo/bar", fs.makeQualified("foo/bar").toString());
			readWriteFile(fs);
			readWriteQualifiedFile(fs);
		}
	}

	@Test
	public void testLocal() throws IOException {
		try (FileSystem fs = FileSystem.getLocal()) {
			assertEquals("file:/" + System.getProperty("user.dir").replace("\\", "/").replaceAll("^/", "") + "/foo/bar",
					fs.makeQualified("foo/bar").toString());
			readWriteFile(fs);
			readWriteQualifiedFile(fs);
		}
	}

}
