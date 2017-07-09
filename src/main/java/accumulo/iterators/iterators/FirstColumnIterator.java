package accumulo.iterators.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class FirstColumnIterator implements SortedKeyValueIterator<Key, Value> {

	private SortedKeyValueIterator<Key, Value> source;
	private boolean done;
	private Range range;
	private Collection<ByteSequence> columnFamilies;
	private boolean inclusive;

	public FirstColumnIterator() {
	}

	private FirstColumnIterator(FirstColumnIterator iter, IteratorEnvironment env) {
		this.source = iter.source.deepCopy(env);
		this.done = iter.done;
		this.range = iter.range;
		this.columnFamilies = iter.columnFamilies;
		this.inclusive = iter.inclusive;
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		this.source = source;
		done = false;
	}

	@Override
	public boolean hasTop() {
		return !done && source.hasTop();
	}

	@Override
	public void next() throws IOException {
		if (done) {
			return;
		}

		Key nextKey = source.getTopKey().followingKey(PartialKey.ROW);
		if (range.afterEndKey(nextKey)) {
			done = true;
			return;
		}
		range = range.clip(new Range(nextKey, null));
		source.seek(range, columnFamilies, inclusive);
	}

	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		this.range = range;
		this.columnFamilies = columnFamilies;
		this.inclusive = inclusive;
		done = false;
		Key startKey = range.getStartKey();
		Key endKey = range.getEndKey();
		Range seekRange = new Range(startKey == null ? null : new Key(startKey.getRow()), range.isStartKeyInclusive(),
				endKey, range.isEndKeyInclusive());
		source.seek(seekRange, columnFamilies, inclusive);
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
		return new FirstColumnIterator(this, env);
	}

}
