package de.hpi.mmds.wiki;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;

public class FileSystem implements Closeable {

	private final org.apache.hadoop.fs.FileSystem fs;

	public FileSystem(org.apache.hadoop.fs.FileSystem fs) {
		this.fs = fs;
	}

	public static FileSystem get(URI uri) throws IOException {
		org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, new Configuration());
		fs.setWorkingDirectory(new Path("/"));
		return new FileSystem(fs);
	}

	public static FileSystem get(String uri) throws IOException {
		return FileSystem.get(URI.create(uri));
	}

	public static FileSystem getLocal() throws IOException {
		return new FileSystem(org.apache.hadoop.fs.FileSystem.get(new Configuration()));
	}

	@Override
	public void close() throws IOException {
		fs.close();
	}

	public BufferedWriter create(String file) throws IOException {
		return create(new Path(file));
	}

	public BufferedWriter create(Path file) throws IOException {
		return new BufferedWriter(new OutputStreamWriter(fs.create(file)));
	}

	public boolean delete(String file) throws IOException {
		return delete(new Path(file));
	}

	public boolean delete(String file, boolean recursive) throws IOException {
		return delete(new Path(file), recursive);
	}

	public boolean delete(Path file) throws IOException {
		return delete(file, true);
	}

	public boolean delete(Path file, boolean recursive) throws IOException {
		return fs.delete(file, recursive);
	}

	public boolean exists(String file) throws IOException {
		return exists(new Path(file));
	}

	public boolean exists(Path file) throws IOException {
		return fs.exists(file);
	}

	public Path makeQualified(String path) {
		return makeQualified(new Path(path));
	}

	public Path makeQualified(Path path) {
		return path.makeQualified(fs.getUri(), fs.getWorkingDirectory());
	}

	public BufferedReader read(String file) throws IOException {
		return read(new Path(file));
	}

	public BufferedReader read(Path file) throws IOException {
		return new BufferedReader(new InputStreamReader(fs.open(file)));
	}
}
