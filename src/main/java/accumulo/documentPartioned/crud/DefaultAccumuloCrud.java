package accumulo.documentPartioned.crud;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.Text;

import accumulo.tableDesign.data.User;

public class DefaultAccumuloCrud implements AccumuloCrud {

	private static final String EMPTY_STRING = "";
	private static final Authorizations auth = Authorizations.EMPTY;
	private static final int N_GRAM_LENGTH = 3;
	private static final int NR_SHARDS = 10;
	private static final Range ALL_RANGE = new Range();

	private Connector connector;
	private String dataTable;
	private String indexTable;
	private MultiTableBatchWriter multiWriter;
	private BatchWriter dataWriter;
	private BatchWriter indexWriter;
	private BatchScanner dataScanner;
	private Scanner indexScanner;

	public DefaultAccumuloCrud(Connector connector, String dataTable, String indexTable) {
		this.connector = connector;
		this.dataTable = dataTable;
		this.indexTable = indexTable;
	}

	@Override
	public void insert(User user) {
		if (dataWriter == null) {
			initializeWriter(dataTable, indexTable);
		}
		String userId = Integer.toString(user.hashCode());

		try {
			dataWriter.addMutation(createDataTableMutation(userId, user));
			indexWriter.addMutation(createIndexTableMutation(userId, user));
			multiWriter.flush();
		} catch (MutationsRejectedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void showWholeTable(String tableName) {
		try (Scanner scanner = connector.createScanner(tableName, auth)) {
			for (Entry<Key, Value> entry : scanner) {
				System.out.println(entry);
			}
		} catch (TableNotFoundException e) {
			System.out.println("Table '" + tableName + "' not exists!");
		}
	}

	@Override
	public List<String> queryExact(String fieldName, String fieldValue) {
		if (indexScanner == null) {
			initializeIndexScanner();
		} else {
			indexScanner.clearColumns();
		}
		indexScanner.setRange(ALL_RANGE);
		indexScanner.fetchColumnFamily(new Text("t:" + fieldName + ":" + fieldValue));
		return fetchDocuments(fetchIds());
	}

	@Override
	public List<String> queryPrefix(String fieldName, String fieldValue) {
		if (fieldValue.length() < N_GRAM_LENGTH) {
			throw new IllegalArgumentException("Length of queried prefix must be longer than " + N_GRAM_LENGTH);
		}
		return queryRegex(fieldName, fieldValue, fieldValue + "(.*)");
	}

	@Override
	public List<String> querySuffix(String fieldName, String fieldValue) {
		if (fieldValue.length() < N_GRAM_LENGTH) {
			throw new IllegalArgumentException("Length of queried suffix must be longer than " + N_GRAM_LENGTH);
		}
		return queryRegex(fieldName, fieldValue, "(.*)" + fieldValue);
	}

	@Override
	public List<String> querySubString(String fieldName, String fieldValue) {
		if (fieldValue.length() < N_GRAM_LENGTH) {
			throw new IllegalArgumentException("Length of queried substring must be longer than " + N_GRAM_LENGTH);
		}
		return queryRegex(fieldName, fieldValue, "(.*)" + fieldValue + "(.*)");
	}

	@Override
	public synchronized void close() {
		if (indexScanner != null) {
			indexScanner.close();
		}
		if (dataScanner != null) {
			dataScanner.close();
		}
		if (multiWriter != null) {
			try {
				multiWriter.close();
			} catch (MutationsRejectedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public List<String> queryMultiExact(List<Pair<String, String>> fields) {
		if (indexScanner == null) {
			initializeIndexScanner();
		} else {
			indexScanner.clearColumns();
		}
		indexScanner.setRange(ALL_RANGE);
		IteratorSetting setting = new IteratorSetting(70, IntersectingIterator.class);
		Text[] columns = new Text[fields.size()];
		int i = 0;
		for (Pair<String, String> entry : fields) {
			columns[i] = new Text("t:" + entry.getFirst() + ":" + entry.getSecond());
			indexScanner.fetchColumnFamily(columns[i]);
			i++;
		}
		IntersectingIterator.setColumnFamilies(setting, columns);
		// indexScanner.addScanIterator(setting);
		Set<String> ids = fetchIds();
		indexScanner.clearScanIterators();
		return fetchDocuments(ids);
	}

	// --- initializing --------------
	private synchronized void initializeWriter(String dataTable, String indexTable) {
		if (dataWriter == null) {
			try {
				multiWriter = connector.createMultiTableBatchWriter(new BatchWriterConfig());
				dataWriter = multiWriter.getBatchWriter(dataTable);
				indexWriter = multiWriter.getBatchWriter(indexTable);
			} catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
				throw new RuntimeException(e);
			}
		}
	}

	// --- inserting -----------------
	private Mutation createDataTableMutation(String id, User user) {
		Mutation dataMut = new Mutation(id);
		dataMut.put(EMPTY_STRING, EMPTY_STRING, user.toString());
		return dataMut;
	}

	private Mutation createIndexTableMutation(String id, User user) {
		String shardId = Integer.toString(Integer.parseInt(id) % NR_SHARDS);

		Mutation mutation = new Mutation(shardId);

		// exact terms
		mutation.put("t:" + "name:" + user.getName(), id, EMPTY_STRING);
		mutation.put("t:" + "age:" + user.getAge(), id, EMPTY_STRING);
		mutation.put("t:" + "city:" + user.getCity(), id, EMPTY_STRING);

		// n-grams
		for (String ngram : createNGrams(user.getName(), N_GRAM_LENGTH)) {
			mutation.put("n:" + "name:" + ngram, id, user.getName());
		}
		for (String ngram : createNGrams(user.getCity(), N_GRAM_LENGTH)) {
			mutation.put("n:" + "city:" + ngram, id, user.getCity());
		}
		return mutation;
	}

	private List<String> createNGrams(String word, int nGramLength) {
		if (word.length() <= nGramLength) {
			return Collections.singletonList(word);
		}
		List<String> results = new ArrayList<>();
		for (int i = 0; i <= word.length() - nGramLength; i++) {
			results.add(word.substring(i, i + nGramLength));
		}
		return results;
	}

	// --- reading
	private synchronized void initializeIndexScanner() {
		if (indexScanner == null) {
			try {
				indexScanner = connector.createScanner(indexTable, auth);
			} catch (TableNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private synchronized void initializeDataScanner() {
		if (dataScanner == null) {
			try {
				dataScanner = connector.createBatchScanner(dataTable, auth, 10);
			} catch (TableNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private Set<String> fetchIds() {
		Set<String> result = new HashSet<>();
		for (Entry<Key, Value> entry : indexScanner) {
			result.add(entry.getKey().getColumnQualifier().toString());
		}
		return result;
	}

	private List<String> fetchDocuments(Collection<String> userIds) {
		if (dataScanner == null) {
			initializeDataScanner();
		} else {
			dataScanner.clearColumns();
		}

		indexScanner.clearColumns();
		if (userIds.isEmpty()) {
			return Collections.emptyList();
		}
		List<Range> ranges = new ArrayList<>(userIds.size());
		for (String userId : userIds) {
			ranges.add(new Range(userId));
		}
		List<String> results = new ArrayList<>(ranges.size());
		dataScanner.setRanges(ranges);
		for (Entry<Key, Value> entry : dataScanner) {
			results.add(entry.getValue().toString());
		}
		return results;
	}

	private List<String> queryRegex(String fieldName, String fieldValue, String regex) {
		if (indexScanner == null) {
			initializeIndexScanner();
		} else {
			indexScanner.clearColumns();
		}
		indexScanner.setRange(ALL_RANGE);
		IteratorSetting setting = new IteratorSetting(70, RegExFilter.class,
				Collections.singletonMap("valueRegex", regex));
		indexScanner.addScanIterator(setting);

		Set<String> ids = new HashSet<>();
		for (String ngram : createNGrams(fieldValue, N_GRAM_LENGTH)) {
			indexScanner.clearColumns();
			indexScanner.fetchColumnFamily(new Text("n:" + fieldName + ":" + ngram));
			ids.addAll(fetchIds());
		}
		indexScanner.clearScanIterators();
		return fetchDocuments(ids);
	}
}
