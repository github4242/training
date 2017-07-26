package accumulo.queryLogic.iterators;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public abstract class QueryIterator implements SortedKeyValueIterator<Key, Value>, Serializable {

	private static final long serialVersionUID = -3113874723026572949L;
	protected Map<String, String> options;
	protected SortedKeyValueIterator<Key, Value> source;

	public void addOptions(Map<String, String> options) {
		if (this.options == null) {
			initializeOptions();
		}
		options.putAll(options);
	}

	protected synchronized void initializeOptions() {
		if (options == null) {
			options = new HashMap<>(2);
		}
	}

	public abstract IteratorSetting getIteratorSetting(int priority);

	@Override
	public abstract QueryIterator deepCopy(IteratorEnvironment env);

	public abstract String serialize() throws IOException;

	public void setSource(SortedKeyValueIterator<Key, Value> source) {
		this.source = source;
	}

}
