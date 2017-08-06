package accumulo.queryLogic.iterators;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.net.util.Base64;

public abstract class TermIterator extends QueryIterator {

	private static final long serialVersionUID = 1L;
	private String fieldName;
	private String fieldValue;
	private String prefix;

	protected TermIterator(String prefix) {
		this.prefix = prefix;
	}

	protected TermIterator(String prefix, String fieldName, String fieldValue) {
		this.prefix = prefix;
		this.fieldName = fieldName;
		this.fieldValue = fieldValue;
	}

	protected TermIterator(SortedKeyValueIterator<Key, Value> source, String prefix, String fieldName,
			String fieldValue) {
		this.source = source;
		this.prefix = prefix;
		this.fieldName = fieldName;
		this.fieldValue = fieldValue;
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
			throws IOException {
		this.source = source;
		if (fieldName == null) {
			fieldName = options.get("fieldName");
		}
		if (fieldValue == null) {
			fieldValue = options.get("fieldValue");
		}
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
		String colFam = prefix + ":" + fieldName + ":" + fieldValue;
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
	public QueryIterator deepCopy(IteratorEnvironment env) {
		return new ExactTermIterator(source, fieldName, fieldValue);
	}

	@Override
	public IteratorSetting getIteratorSetting(int priority) {
		IteratorSetting setting = new IteratorSetting(priority, this.getClass());
		setting.addOption("fieldName", fieldName);
		setting.addOption("fieldValue", fieldValue);
		return setting;
	}

	@Override
	public String serialize() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
		DataOutputStream output = new DataOutputStream(bos);
		output.writeUTF(this.getClass().getName());
		output.writeUTF(fieldName);
		output.writeUTF(fieldValue);
		output.close();
		return Base64.encodeBase64String(bos.toByteArray());
	}

}
