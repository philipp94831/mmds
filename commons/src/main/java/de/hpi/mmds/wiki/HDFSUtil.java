package de.hpi.mmds.wiki;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;

public class HDFSUtil implements Closeable {

	private final FileSystem fs;

	public HDFSUtil(URI uri) throws IOException {
		this.fs = FileSystem.get(uri, new Configuration());
	}

	@Override
	public void close() throws IOException {
		fs.close();
	}

	public BufferedWriter write(Path file) throws IOException {
		return new BufferedWriter(new OutputStreamWriter(fs.create(file)));
	}

	public BufferedReader read(Path file) throws IOException {
		return new BufferedReader(new InputStreamReader(fs.open(file)));
	}

	public boolean delete(Path file) throws IOException {
		return delete(file, true);
	}

	public boolean delete(Path file, boolean recursive) throws IOException {
		return fs.delete(file, recursive);
	}

	public boolean exists(Path file) throws IOException {
		return fs.exists(file);
	}
}
