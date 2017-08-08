package accumulo.queryLogic.iterators;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.commons.math3.util.Pair;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accumulo.queryLogic.util.ProxyEnv;

public class AndIterator extends QueryIterator {

	private static final Logger LOGGER = LoggerFactory.getLogger(AndIterator.class);
	private static final long serialVersionUID = 1L;

	private List<QueryIterator> childIterators;
	private QueryIterator lastChild;
	private boolean noResults = true;
	private Iterator<Pair<Key, Value>> resultsIterator;
	private Key topKey = null;
	private Value topValue = null;
	private boolean hasTop = false;
	private List<Text> docIds = new ArrayList<>();

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
		ProxyEnv proxyEnv = new ProxyEnv(env);
		for (int i = 0; i < options.size(); i++) {
			childIterators.add(deserializeChild(options.get(Integer.toString(i))));
			if (i == 0) {
				childIterators.get(0).setSource(source);
				;
			} else {
				childIterators.get(i).setSource(source.deepCopy(proxyEnv));
			}
		}
		lastChild = childIterators.size() == 0 ? null : childIterators.get(childIterators.size() - 1);
		this.source = source;
	}

	@Override
	public boolean hasTop() {
		if (noResults) {
			return false;
		} else {
			return hasTop;
		}
	}

	@Override
	public void next() throws IOException {
		if (lastChild == null || noResults) {
			return;
		} else {
			if (resultsIterator.hasNext()) {
				Pair<Key, Value> res = resultsIterator.next();
				hasTop = true;
				topKey = res.getFirst();
				topValue = res.getSecond();
			} else {
				hasTop = false;
				topKey = null;
				topValue = null;
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		List<Pair<Key, Value>> results = new ArrayList<>();
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

		for (int i = 0; i < nrChilds - 1; i++) {
			List<Range> tempRanges = new ArrayList<>();
			QueryIterator child = childIterators.get(i);
			List<Text> tempDocIds = new ArrayList<>();
			for (Range thisRange : ranges) {
				child.seek(thisRange, columnFamilies, inclusive);
				if (!child.hasTop()) {
					continue;
				}
				noResults = false;
				while (child.hasTop()) {
					List<Range> thisRow = Collections.singletonList(Range.exact(child.getTopKey().getRow()));
					tempRanges = Range.mergeOverlapping(ListUtils.union(tempRanges, thisRow));

					if (i == 0) {
						docIds.add(child.getTopKey().getColumnQualifier());
					} else {
						tempDocIds.add(child.getTopKey().getColumnQualifier());
					}
					child.next();
				}
			}
			if (i != 0) {
				docIds.retainAll(tempDocIds);
			}
			ranges = tempRanges;
		}
		for (Range thisRange : ranges) {
			lastChild.seek(thisRange, columnFamilies, inclusive);
			while (lastChild.hasTop()) {
				if (docIds.contains(lastChild.getTopKey().getColumnQualifier())) {
					results.add(new Pair<>(lastChild.getTopKey(), lastChild.getTopValue()));
				}
				lastChild.next();
			}
		}
		if (!results.isEmpty()) {
			hasTop = true;
			resultsIterator = results.iterator();
			Pair<Key, Value> firstEntry = resultsIterator.next();
			topKey = firstEntry.getFirst();
			topValue = firstEntry.getSecond();
		} else {
			hasTop = false;
		}
	}

	@Override
	public Key getTopKey() {
		if (noResults) {
			return null;
		} else {
			return topKey;
		}
	}

	@Override
	public Value getTopValue() {
		if (noResults) {
			return null;
		} else {
			return topValue;
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
