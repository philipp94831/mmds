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
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

public class HDFSUtilTest {

	private static HDFSUtil fs;
	private static MiniDFSCluster hdfsCluster;

	@BeforeClass
	public static void setupClass() throws URISyntaxException, IOException {
		Configuration conf = new Configuration();
		File baseDir = new File("./target/hdfs/").getAbsoluteFile();
		if(baseDir.exists()) {
			FileUtils.forceDelete(baseDir);
		}
		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		hdfsCluster = builder.build();
		fs = new HDFSUtil(new URI("hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/"));
	}

	@AfterClass
	public static void tearDownClass() throws IOException {
		fs.close();
		hdfsCluster.shutdown();
	}

	@Test
	public void testHDFS() throws IOException {
		Path pt = new Path("/data/mmds16/wiki/meta.txt");
		if (fs.exists(pt)) {
			fs.delete(pt);
		}
		try (BufferedWriter out = fs.write(pt)) {
			out.write("hello");
		}
		try (BufferedReader in = fs.read(pt)) {
			int i = 0;
			for (String line = in.readLine(); line != null; line = in.readLine(), i++) {
				assertEquals(5, line.length());
				assertEquals("hello", line);
			}
			assertEquals(1, i);
		}
		if (fs.exists(pt)) {
			fs.delete(pt);
		}
	}

}
