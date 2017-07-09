package accumulo.queryLogic.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class ExactTermIterator implements SortedKeyValueIterator<Key, Value> {

	private SortedKeyValueIterator<Key, Value> source;
	private String fieldName;
	private String fieldValue;

	public ExactTermIterator() {
	}

	public ExactTermIterator(SortedKeyValueIterator<Key, Value> source, String fieldName, String fieldValue) {
		this.source = source;
		this.fieldName = fieldName;
		this.fieldValue = fieldValue;
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		this.source = source;
		fieldName = options.get("fieldName");
		fieldValue = options.get("fieldValue");
		if (fieldName == null || fieldValue == null) {
			throw new IllegalArgumentException("Option entries 'fieldName' and 'fieldValue' are mandatory.");
		}
	}

	@Override
	public boolean hasTop() {
		return source.hasTop();
	}

	@Override
	public void next() throws IOException {
		source.next();

	}

	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		String colFam = "t:" + fieldName + ":" + fieldValue;
		source.seek(range, Collections.singletonList(new ArrayByteSequence(colFam.getBytes())), true);
	}

	@Override
	public Key getTopKey() {
		return source.getTopKey();
	}

	@Override
	public Value getTopValue() {
		return source.getTopValue();
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new ExactTermIterator(source, fieldName, fieldValue);
	}

}
