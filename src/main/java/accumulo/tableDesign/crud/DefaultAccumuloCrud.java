package accumulo.tableDesign.crud;

import java.util.ArrayList;
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
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import accumulo.tableDesign.data.User;

public class DefaultAccumuloCrud implements AccumuloCrud {

	private static final byte[] EMPTY_BYTES = "".getBytes();
	private static final Authorizations auth = Authorizations.EMPTY;
	private static final int N_GRAM_LENGTH = 3;

	private Connector connector;
	private String dataTable;
	private String indexTable;
	private MultiTableBatchWriter multiWriter;
	private BatchWriter dataWriter;
	private BatchWriter indexWriter;
	private BatchScanner dataScanner;
	private Scanner indexScanner;
	private Set<String> userIds = new HashSet<>();

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
			indexWriter.addMutations(createIndexTableMutation(userId, user));
			multiWriter.flush();
		} catch (MutationsRejectedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void showWholeTable(String table) {
		try (Scanner scanner = connector.createScanner(table, auth)) {
			for (Entry<Key, Value> entry : scanner) {
				System.out.println(entry);
			}
		} catch (TableNotFoundException e) {
			System.out.println("Table '" + table + "' not exists!");
		}
	}

	@Override
	public List<String> queryExact(String fieldName, String fieldValue) {
		if (indexScanner == null) {
			initializeIndexScanner();
		} else {
			indexScanner.clearColumns();
		}
		Range range = new Range("t:" + fieldValue);
		indexScanner.setRange(range);
		indexScanner.fetchColumnFamily(new Text(fieldName.getBytes()));

		userIds.addAll(fetchUserIds());

		return fetchDocuments();
	}

	@Override
	public List<String> queryPrefix(String fieldName, String fieldValue) {
		return queryPrefixSuffix(fieldName, "t:" + fieldValue);
	}

	@Override
	public List<String> querySuffix(String fieldName, String fieldValue) {
		return queryPrefixSuffix(fieldName, "r:" + fieldValue);
	}

	@Override
	public List<String> querySubString(String fieldName, String fieldValue) {
		if (fieldValue.length() < N_GRAM_LENGTH) {
			System.out.println("Substring to short. Must have at least " + N_GRAM_LENGTH + " characters!");
		}
		if (indexScanner == null) {
			initializeIndexScanner();
		} else {
			indexScanner.clearColumns();
		}
		for (String ngram : createNGrams(fieldValue, N_GRAM_LENGTH)) {
			userIds.addAll(queryNGram(fieldName, fieldValue, ngram));
		}
		return fetchDocuments();
	}

	private Set<String> queryNGram(String fieldName, String fieldValue, String ngram) {
		Range range = new Range("n:" + ngram);
		indexScanner.setRange(range);
		indexScanner.fetchColumnFamily(new Text(fieldName.getBytes()));

		Set<String> result = new HashSet<>();
		result.addAll(fetchUserIdsNGram(fieldValue));
		return result;
	}

	private List<String> queryPrefixSuffix(String fieldName, String fieldValue) {
		if (indexScanner == null) {
			initializeIndexScanner();
		} else {
			indexScanner.clearColumns();
		}
		Range range = new Range(fieldValue, true, fieldValue + '{', false);
		indexScanner.setRange(range);
		indexScanner.fetchColumnFamily(new Text(fieldName.getBytes()));

		userIds.addAll(fetchUserIds());
		return fetchDocuments();
	}

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

	private Mutation createDataTableMutation(String id, User user) {
		Mutation dataMut = new Mutation(id);
		dataMut.put(EMPTY_BYTES, EMPTY_BYTES, user.toString().getBytes());
		return dataMut;
	}

	private List<Mutation> createIndexTableMutation(String id, User user) {
		List<Mutation> list = new ArrayList<>();

		// original order
		Mutation nameMut = new Mutation("t:" + user.getName());
		nameMut.put("name".getBytes(), id.getBytes(), EMPTY_BYTES);
		list.add(nameMut);

		Mutation ageMut = new Mutation("t:" + Integer.toString(user.getAge()));
		ageMut.put("age".getBytes(), id.getBytes(), EMPTY_BYTES);
		list.add(ageMut);

		Mutation cityMut = new Mutation("t:" + user.getCity());
		cityMut.put("city".getBytes(), id.getBytes(), EMPTY_BYTES);
		list.add(cityMut);

		// reverse order of name and city
		Mutation revNameMut = new Mutation("r:" + new StringBuffer(user.getName()).reverse().toString());
		revNameMut.put("name".getBytes(), id.getBytes(), EMPTY_BYTES);
		list.add(revNameMut);

		Mutation revCityMut = new Mutation("r:" + new StringBuffer(user.getCity()).reverse().toString());
		revCityMut.put("city".getBytes(), id.getBytes(), EMPTY_BYTES);
		list.add(revCityMut);

		// n-gram of name and city
		for (String ngram : createNGrams(user.getName(), N_GRAM_LENGTH)) {
			Mutation nGramNameMut = new Mutation("n:" + ngram);
			nGramNameMut.put("name".getBytes(), id.getBytes(), user.getName().getBytes());
			list.add(nGramNameMut);
		}

		for (String ngram : createNGrams(user.getCity(), N_GRAM_LENGTH)) {
			Mutation nGramCityMut = new Mutation("n:" + ngram);
			nGramCityMut.put("city".getBytes(), id.getBytes(), user.getCity().getBytes());
			list.add(nGramCityMut);
		}

		return list;
	}

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

	private Set<String> fetchUserIds() {
		Set<String> result = new HashSet<>();
		for (Entry<Key, Value> entry : indexScanner) {
			result.add(entry.getKey().getColumnQualifier().toString());
		}
		return result;
	}

	private Set<String> fetchUserIdsNGram(String fieldValue) {
		Set<String> result = new HashSet<>();
		for (Entry<Key, Value> entry : indexScanner) {
			if (entry.getValue().toString().matches("(.*)" + fieldValue + "(.*)")) {
				result.add(entry.getKey().getColumnQualifier().toString());
			}
		}
		return result;
	}

	private List<String> fetchDocuments() {
		if (dataScanner == null) {
			initializeDataScanner();
		} else {
			dataScanner.clearColumns();
		}

		if (userIds.isEmpty()) {
			return Collections.emptyList();
		}
		List<Range> ranges = new ArrayList<>(userIds.size());
		for (String userId : userIds) {
			ranges.add(new Range(userId));
		}
		userIds.clear();
		List<String> results = new ArrayList<>(ranges.size());
		dataScanner.setRanges(ranges);
		for (Entry<Key, Value> entry : dataScanner) {
			results.add(entry.getValue().toString());
		}
		return results;
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

}
