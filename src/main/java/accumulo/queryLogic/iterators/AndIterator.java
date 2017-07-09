package accumulo.queryLogic.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

public class AndIterator implements SortedKeyValueIterator<Key, Value> {

	private static final Text EMPTY_TEXT = new Text("");

	private List<SortedKeyValueIterator<Key, Value>> childIterators;
	private SortedKeyValueIterator<Key, Value> firstChildIterator;
	private boolean hasTop;
	private Key topKey;
	private Value topValue;
	private SortedKeyValueIterator<Key, Value> source;

	public AndIterator() {
	}

	public AndIterator(IteratorEnvironment env, SortedKeyValueIterator<Key, Value> firstChildIterator,
			List<SortedKeyValueIterator<Key, Value>> childIterators) {
		this.childIterators = new ArrayList<>(childIterators.size() + 1);
		this.childIterators.add(firstChildIterator.deepCopy(env));
		for (SortedKeyValueIterator<Key, Value> iterator : childIterators) {
			this.childIterators.add(iterator.deepCopy(env));
		}
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		for (SortedKeyValueIterator<Key, Value> iterator : childIterators) {
			iterator.init(source, options, env);
		}
		firstChildIterator = childIterators.remove(0);
		hasTop = false;
		this.source = source;
	}

	@Override
	public boolean hasTop() {
		return hasTop;
	}

	@Override
	public void next() throws IOException {
		source.next();
	}

	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		firstChildIterator.seek(range, columnFamilies, inclusive);

		// find relevant shardId and docId
		Key currentKey = firstChildIterator.getTopKey();
		Text shardId = currentKey.getRow();
		Text docId;
		if (shardId != null) {
			docId = currentKey.getColumnQualifier();
		} else {
			topKey = null;
			topValue = null;
			hasTop = false;
			return;
		}
		// test rest of child iterators
		for (SortedKeyValueIterator<Key, Value> childIterator : childIterators) {
			boolean found = false;
			while (!found) {
				childIterator.seek(Range.exact(shardId), columnFamilies, inclusive);
				if (childIterator.getTopKey().getColumnQualifier().equals(docId)) {
					found = true;
				} else if (!childIterator.hasTop()) {
					topKey = null;
					topValue = null;
					hasTop = false;
					return;
				}
			}
		}
		topKey = new Key(shardId, EMPTY_TEXT, docId);
		topValue = new Value(EMPTY_TEXT);
		hasTop = true;
	}

	@Override
	public Key getTopKey() {
		return topKey;
	}

	@Override
	public Value getTopValue() {
		return topValue;
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new AndIterator(env, firstChildIterator, childIterators);
	}

}
