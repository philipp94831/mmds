package de.hpi.mmds.wiki.de.hpi.mmds.wiki.spark;

import de.hpi.mmds.wiki.Spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SparkTest {

	@Test
	public void test() {
		try (JavaSparkContext jsc = Spark.newApp("Foo").setMaster("local[4]").setWorkerMemory("128m").context()) {
			assertEquals(jsc.appName(), "Foo");
			assertEquals(jsc.master(), "local[4]");
			assertEquals(jsc.getConf().get("spark.executor.memory"), "128m");
		}
	}

}
