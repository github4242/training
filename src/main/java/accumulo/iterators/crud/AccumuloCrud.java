package accumulo.iterators.crud;

import java.io.Closeable;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

public interface AccumuloCrud extends Closeable {

	public void insert(byte[] row, byte[] cf, byte[] cq, ColumnVisibility vis, byte[] value);

	public Entry<Key, Value> readEntry(byte[] row, byte[] cf, byte[] cq);

	public void delete(byte[] row, byte[] cf, byte[] cq);

}
