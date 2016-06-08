package de.hpi.mmds.wiki.spark;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public final class KeyFilter<T> implements Function<Tuple2<T, T>, Boolean> {

	private static final long serialVersionUID = -3461545223112967623L;
	private final T value;

	public KeyFilter(T filter) {
		this.value = filter;
	}

	@Override
	public Boolean call(Tuple2<T, T> t) throws Exception {
		return t._1.equals(value);
	}
}