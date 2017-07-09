package accumulo.iterators.app;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import accumulo.iterators.iterators.FirstColumnIterator;
import accumulo.util.AccumuloClient;
import accumulo.util.MiniAccumuloClient;

public class WholeIteratorExample {

	private static final String PASSWORD = "pass";
	private static final String TABLE = "table";

	public static void main(String[] args) {

		try (AccumuloClient accumuloClient = new MiniAccumuloClient(PASSWORD)) {
			Connector connector = accumuloClient.getConnector();
			TableOperations to = connector.tableOperations();
			to.create(TABLE);
			BatchWriter writer = connector.createBatchWriter(TABLE, new BatchWriterConfig());
			for (int i = 0; i < 100; i++) {
				Mutation m = new Mutation("row" + String.format("%02d", i));
				for (int j = 0; j < 100; j++) {
					m.put("", "col" + j, Integer.toString(i * j));
				}
				writer.addMutation(m);
			}
			writer.close();

			Scanner scanner = connector.createScanner(TABLE, Authorizations.EMPTY);
			scanner.setRange(new Range("row50", "row60"));
			IteratorSetting setting = new IteratorSetting(30, "wri", WholeRowIterator.class);
			scanner.addScanIterator(setting);
			scanner.fetchColumn(new Text("".getBytes()), new Text("col59".getBytes()));

			for (Entry<Key, Value> entry : scanner) {
				System.out.println(entry);
				Map<Key, Value> row = WholeRowIterator.decodeRow(entry.getKey(), entry.getValue());
				System.out.println(row.keySet());
				System.out.println(row.values());
			}

			// ---- test firstColumnIterator
			scanner.clearColumns();
			scanner.clearScanIterators();
			IteratorSetting setting2 = new IteratorSetting(30, "fci", FirstColumnIterator.class);
			scanner.addScanIterator(setting2);
			for (Entry<Key, Value> entry : scanner) {
				System.out.println(entry);
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
