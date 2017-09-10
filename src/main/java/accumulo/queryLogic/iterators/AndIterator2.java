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
import java.util.Optional;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.math3.util.Pair;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accumulo.queryLogic.util.ProxyEnv;
import accumulo.queryLogic.util.RingIterator;

public class AndIterator2 extends QueryIterator {

	private static final Logger LOGGER = LoggerFactory.getLogger(AndIterator2.class);
	private static final long serialVersionUID = 1L;

	private RingIterator<QueryIterator> childIterators;
	private final int nrChilds;
	private boolean inclusive;
	// private QueryIterator lastChild;
	// private boolean noResults = true;
	// private Iterator<Pair<Key, Value>> resultsIterator;
	private Key topKey = null;
	private Value topValue = null;
	private boolean hasTop = false;
	private Range range;
	// private List<Text> docIds = new ArrayList<>();

	public AndIterator2() {
		childIterators = new RingIterator<>(new ArrayList<>());
		nrChilds = 0;
	}

	public AndIterator2(IteratorEnvironment env, List<QueryIterator> childIterators) {
		List<QueryIterator> childs = new ArrayList<>(childIterators.size());
		for (QueryIterator iterator : childIterators) {
			childs.add(iterator.deepCopy(env));
		}
		this.childIterators = new RingIterator<>(childs);
		nrChilds = childIterators.size();
	}

	public AndIterator2(List<QueryIterator> childIterators) {
		this.childIterators = new RingIterator<>(childIterators);
		nrChilds = childIterators.size();
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		ProxyEnv proxyEnv = new ProxyEnv(env);
		for (int i = 0; i < options.size(); i++) {
			childIterators.getList().add(deserializeChild(options.get(Integer.toString(i))));
			if (i == 0) {
				childIterators.getList().get(0).setSource(source);
				;
			} else {
				childIterators.getList().get(i).setSource(source.deepCopy(proxyEnv));
			}
		}
		this.source = source;
	}

	@Override
	public boolean hasTop() {
		return hasTop;
	}

	@Override
	public void next() throws IOException {
		Optional<Pair<Key, Value>> res = advanceToNextMatch(range);
		if (!res.isPresent()) {
			hasTop = false;
			topValue = null;
			topKey = null;
			return;
		}
		hasTop = true;
		topKey = res.get().getKey();
		topValue = res.get().getValue();
		this.range = range.clip(new Range(topKey, null));
	}

	@Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		this.inclusive = inclusive;
		Optional<Pair<Key, Value>> res = advanceToNextMatch(range);
		if (!res.isPresent()) {
			hasTop = false;
			topValue = null;
			topKey = null;
			return;
		}
		hasTop = true;
		topKey = res.get().getKey();
		topValue = res.get().getValue();
		this.range = range.clip(new Range(topKey, null));
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
		return new AndIterator2(env, childIterators.getList());
	}

	@Override
	public IteratorSetting getIteratorSetting(int priority) {
		Map<String, String> options = new HashMap<>(childIterators.getList().size());
		try {
			int i = 0;
			for (QueryIterator iter : childIterators.getList()) {
				options.put(Integer.toString(i), iter.serialize());
				i++;
			}
			IteratorSetting setting = new IteratorSetting(priority, AndIterator2.class);
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

	Optional<Pair<Key, Value>> advanceToNext(Range range) {
		if (!childIterators.hasNext()) {
			return Optional.empty();
		}
		QueryIterator child = childIterators.next();
		try {
			child.seek(range, null, inclusive);
			return Optional.of(new Pair<>(child.getTopKey(), child.getTopValue()));
		} catch (IOException e) {
			String msg = e.getMessage();
			LOGGER.error(msg);
			return Optional.empty();
		}
	}

	Optional<Pair<Key, Value>> advanceToNextMatch(Range range) {
		int found = 0;
		Text currentDocId = range.getStartKey().getColumnQualifier();
		Range currentRange = new Range(range);
		while (true) {
			Optional<Pair<Key, Value>> childResult = advanceToNext(currentRange);
			if (!childResult.isPresent()) {
				return Optional.empty();
			}

			Text thisDocId = childResult.get().getKey().getColumnQualifier();
			if (currentDocId.equals(thisDocId)) {
				found++;
			} else {
				found = 0;
				currentRange = currentRange.clip(new Range(childResult.get().getKey(), null));
			}
			if (found == nrChilds) {
				return childResult;
			}
		}
	}

}
