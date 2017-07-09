package accumulo.iterators.combiners;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.ValueFormatException;

public class QuadraticSummingCombiner extends TypedValueCombiner<Integer> {

	@Override
	public Integer typedReduce(Key key, Iterator<Integer> iter) {
		Integer res = 0;
		while (iter.hasNext()) {
			Integer val = iter.next();
			res += val * val;
		}
		return res;
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		super.init(source, options, env);
		setEncoder(new IntegerEncoder());
	}

	public static class IntegerEncoder implements Encoder<Integer> {

		public IntegerEncoder() {
		};

		@Override
		public byte[] encode(Integer v) {
			return ByteBuffer.allocate(4).putInt(v).array();
		}

		@Override
		public Integer decode(byte[] b) throws ValueFormatException {
			return ByteBuffer.wrap(b).getInt();
		}

	}

}
