package accumulo.iterators.formatters;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.format.FormatterConfig;

public class LongFormatter implements Formatter {

	private Iterator<Entry<Key, Value>> iter;

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public String next() {
		byte[] val = iter.next().getValue().get();
		return derserializeValue(val);
	}

	@Override
	public void initialize(Iterable<Entry<Key, Value>> scanner, FormatterConfig config) {
		iter = scanner.iterator();
	}

	public static String derserializeValue(byte[] val) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.put(val);
		buffer.flip();
		return Long.toString(buffer.getLong());
	}

}
