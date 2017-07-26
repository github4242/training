package accumulo.queryLogic.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class ExactTermIterator extends TermIterator {

	private static final long serialVersionUID = 1L;

	public ExactTermIterator() {
		super("t");
	}

	public ExactTermIterator(String fieldName, String fieldValue) {
		super("t", fieldName, fieldValue);
	}

	public ExactTermIterator(SortedKeyValueIterator<Key, Value> source, String fieldName, String fieldValue) {
		super(source, "t", fieldName, fieldValue);
	}

}
