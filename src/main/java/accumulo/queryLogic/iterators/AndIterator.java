package accumulo.queryLogic.iterators;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.commons.collections.ListUtils;
import org.apache.commons.net.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AndIterator extends QueryIterator {

	private static final Logger LOGGER = LoggerFactory.getLogger(AndIterator.class);
	private static final long serialVersionUID = 1L;

	private List<QueryIterator> childIterators;
	private QueryIterator lastChild;
	private boolean noResults = true;
	private boolean sourcesSet = false;
	private IteratorEnvironment env;

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
			if (i == 0) {
				childIterators.get(0).init(source, Collections.emptyMap(), env);
			} else {
				// source.deepCopy(env);
			}
		}
		lastChild = childIterators.size() == 0 ? null : childIterators.get(childIterators.size() - 1);
		this.source = source;
		this.env = env;
	}

	@Override
	public boolean hasTop() {
		if (noResults) {
			return false;
		} else {
			return lastChild == null ? false : lastChild.hasTop();
		}
	}

	@Override
	public void next() throws IOException {
		if (lastChild == null || noResults) {
			return;
		} else {
			lastChild.next();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		List<Range> ranges = Collections.singletonList(range);
		int nrChilds = childIterators.size();
		if (nrChilds == 0) {
			return;
		}

		if (nrChilds == 1) {
			noResults = false;
			lastChild.seek(range, columnFamilies, inclusive);
			return;
		}

		if (!sourcesSet) {
			for (int i = 1; i < nrChilds; i++) {
				childIterators.get(i).setSource(source.deepCopy(env));// , Collections.emptyMap(), env);
			}
			sourcesSet = true;
		}

		for (int i = 0; i < nrChilds - 1; i++) {
			List<Range> tempRanges = null;
			QueryIterator child = childIterators.get(i);
			for (Range thisRange : ranges) {
				child.seek(thisRange, columnFamilies, inclusive);
				if (!child.hasTop()) {
					continue;
				}
				noResults = false;
				tempRanges = new ArrayList<>();
				while (child.hasTop()) {
					List<Range> thisRow = Collections.singletonList(Range.exact(child.getTopKey().getRow()));
					tempRanges = Range.mergeOverlapping(ListUtils.union(tempRanges, thisRow));
					child.next();
				}
			}
			ranges = tempRanges;
		}
	}

	@Override
	public Key getTopKey() {
		if (noResults) {
			return null;
		} else {
			return lastChild == null ? null : lastChild.getTopKey();
		}
	}

	@Override
	public Value getTopValue() {
		if (noResults) {
			return null;
		} else {
			return lastChild == null ? null : lastChild.getTopValue();
		}
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
