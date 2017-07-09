package accumulo.iterators.crud;

import java.util.Collections;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class DefaultAccumuloCrud implements AccumuloCrud {

	private static Logger LOGGER = LogManager.getLogger(DefaultAccumuloCrud.class);

	private BatchWriter writer;
	private BatchScanner scanner;
	private BatchDeleter deleter;
	private Connector connector;
	private Authorizations auths;
	private String tablename;

	public DefaultAccumuloCrud(Connector connector, String tablename, Authorizations auths) {
		this.connector = connector;
		this.tablename = tablename;
		this.auths = auths;
	}

	@Override
	public void insert(byte[] row, byte[] cf, byte[] cq, ColumnVisibility vis, byte[] value) {
		try {
			if (writer == null) {
				createWriter();
			}
			Mutation mutation = new Mutation(row);
			mutation.put(cf, cq, vis, value);
			writer.addMutation(mutation);
		} catch (MutationsRejectedException e) {
			LOGGER.error(e.getLocalizedMessage());
			throw new RuntimeException(e);
		}
	}

	@Override
	public Entry<Key, Value> readEntry(byte[] row, byte[] cf, byte[] cq) {
		if (scanner == null) {
			createScanner();
		} else {
			scanner.clearColumns();
		}
		scanner.setRanges(Collections.singleton(new Range(new Text(row))));
		scanner.fetchColumn(new Text(cf), new Text(cq));
		if (scanner.iterator().hasNext()) {
			return scanner.iterator().next();
		} else {
			return null;
		}
	}

	@Override
	public void delete(byte[] row, byte[] cf, byte[] cq) {
		if (deleter == null) {
			createDeleter();
		} else {
			deleter.clearColumns();
		}
		try {
			deleter.setRanges(Collections.singleton(new Range(new Text(row))));
			deleter.fetchColumn(new Text(cf), new Text(cq));
			deleter.delete();
		} catch (TableNotFoundException | MutationsRejectedException e) {
			LOGGER.error(e.getLocalizedMessage());
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized void close() {
		if (scanner != null) {
			scanner.close();
		}
		if (writer != null) {
			try {
				writer.close();
			} catch (MutationsRejectedException e) {
				LOGGER.error(e.getLocalizedMessage());
				throw new RuntimeException(e);
			}
		}
		if (deleter != null) {
			deleter.close();
		}
	}

	private synchronized void createWriter() {
		if (writer == null) {
			try {
				BatchWriterConfig config = new BatchWriterConfig();
				config.setMaxMemory(1_000_000L);
				config.setMaxLatency(100L, TimeUnit.MILLISECONDS);
				writer = connector.createBatchWriter(tablename, config);
			} catch (TableNotFoundException e) {
				LOGGER.error(e.getLocalizedMessage());
				throw new RuntimeException(e);
			}
		}
	}

	private synchronized void createScanner() {
		if (scanner == null) {
			try {
				scanner = connector.createBatchScanner(tablename, auths, 10);
			} catch (TableNotFoundException e) {
				LOGGER.error(e.getLocalizedMessage());
				throw new RuntimeException(e);
			}
		}
	}

	private synchronized void createDeleter() {
		if (deleter == null) {
			try {
				BatchWriterConfig config = new BatchWriterConfig();
				config.setMaxMemory(1_000_000L);
				config.setMaxLatency(100L, TimeUnit.MILLISECONDS);
				deleter = connector.createBatchDeleter(tablename, auths, 10, config);
			} catch (TableNotFoundException e) {
				LOGGER.error(e.getLocalizedMessage());
				throw new RuntimeException(e);
			}
		}
	}

}
