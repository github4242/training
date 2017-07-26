package accumulo.queryLogic.iterators;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AndIterator extends QueryIterator {

	private static final Logger LOGGER = LoggerFactory.getLogger(AndIterator.class);
	private static final long serialVersionUID = 1L;

	private static final Text EMPTY_TEXT = new Text("");

	private List<QueryIterator> childIterators;
	private boolean hasTop;
	private Key topKey;
	private Value topValue;

	public AndIterator() {
		childIterators = new ArrayList<>();
	}

	public AndIterator(IteratorEnvironment env, List<QueryIterator> childIterators) {
		this.childIterators = new ArrayList<>(childIterators.size());
		for (QueryIterator iterator : childIterators) {
			this.childIterators.add(iterator.deepCopy(env));
		}
	}

	public AndIterator(List<QueryIterator> childIterators) {
		this.childIterators = childIterators;
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		for (int i = 0; i < options.size(); i++) {
			childIterators.add(deserializeChild(options.get(Integer.toString(i))));
		}

		hasTop = false;
		topKey = null;
		topValue = null;
		this.source = source;
		for (QueryIterator child : childIterators) {
			child.setSource(source);
		}
	}

	@Override
	public boolean hasTop() {
		return hasTop ? source.hasTop() : false;
	}

	@Override
	public void next() throws IOException {
		childIterators.get(childIterators.size() - 1).next();
	}

	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		boolean isFirst = true;
		// Range currentRange = new Range(range);
		QueryIterator lastIter = null;
		Text docId = null;
		for (QueryIterator child : childIterators) {
			child.seek(range, columnFamilies, inclusive);
			if (isFirst) {
				range = new Range(child.getTopKey().getRow(), range.getEndKey().getRow());
				isFirst = false;
				docId = child.getTopKey().getColumnQualifier();
				lastIter = child;
			} else if (!child.hasTop() || !docId.equals(child.getTopKey().getColumnQualifier())) {

				topKey = null;
				topValue = null;
				hasTop = false;
				return;
			} else {
				lastIter = child;
			}
		}
		if (lastIter != null) {
			topKey = lastIter.getTopKey();
			topValue = lastIter.getTopValue();
			hasTop = lastIter.hasTop();
		} else {
			topKey = null;
			topValue = null;
			hasTop = false;
		}
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
	public QueryIterator deepCopy(IteratorEnvironment env) {
		return new AndIterator(env, childIterators);
	}

	@Override
	public IteratorSetting getIteratorSetting(int priority) {
		Map<String, String> options = new HashMap<>(childIterators.size());
		try {
			int i = 0;
			for (QueryIterator iter : childIterators) {
				options.put(Integer.toString(i), iter.serialize());
				i++;
			}
			IteratorSetting setting = new IteratorSetting(priority, AndIterator.class);
			setting.addOptions(options);
			return setting;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String serialize() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	private QueryIterator deserializeChild(String serIter) {
		ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decodeBase64(serIter));
		DataInputStream input = new DataInputStream(bis);
		try {
			String className = input.readUTF();
			if (className.equals(ExactTermIterator.class.getName())) {
				String fieldName = input.readUTF();
				String fieldValue = input.readUTF();
				QueryIterator iter = (QueryIterator) this.getClass().getClassLoader().loadClass(className)
						.getConstructor(String.class, String.class).newInstance(fieldName, fieldValue);
				return iter;
			} else {
				return null;
			}
		} catch (IOException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException
				| IllegalAccessException | InstantiationException e) {
			LOGGER.error(e.getLocalizedMessage());
			return null;
		}
	}

}
