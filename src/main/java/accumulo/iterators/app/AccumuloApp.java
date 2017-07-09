package accumulo.iterators.app;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import accumulo.iterators.combiners.QuadraticSummingCombiner;
import accumulo.iterators.combiners.QuadraticSummingCombiner.IntegerEncoder;
import accumulo.iterators.crud.AccumuloCrud;
import accumulo.iterators.crud.DefaultAccumuloCrud;
import accumulo.util.AccumuloClient;
import accumulo.util.MiniAccumuloClient;

public class AccumuloApp {

	private static Logger LOGGER = LogManager.getLogger(AccumuloApp.class);
	private static String USER = "user";
	private static String PASSWORD = "pass";
	private static String TABLE1 = "table1";
	private static String TABLE2 = "table2";
	private static List<String> TABLES = Arrays.asList(TABLE1, TABLE2);

	// ---- test data --------
	private static byte[] ROW = "row".getBytes();
	private static byte[] CF = "cf_colFam".getBytes();
	private static byte[] CQ = "colQual".getBytes();
	private static byte[] VAL = "value".getBytes();

	public static void main(String[] args) throws IOException, InterruptedException {

		LOGGER.error("test");

		AccumuloClient accumuloClient = new MiniAccumuloClient(PASSWORD);
		Connector connector = accumuloClient.getConnector();
		Encoder<Integer> encoder = new IntegerEncoder();

		TableOperations to = connector.tableOperations();
		for (String table : TABLES) {
			try {
				to.create(table);
				// to.addConstraint(table, ColFamStartsWithCfConstraint.class.getName());
				Map<String, EnumSet<IteratorScope>> iters = to.listIterators(table);
				for (Entry<String, EnumSet<IteratorScope>> entry : iters.entrySet()) {
					System.out.println(table + " : " + entry.getKey());
				}
				to.removeIterator(table, "vers", EnumSet.allOf(IteratorScope.class));
				//
				// IteratorSetting scSetting = new IteratorSetting(70, "sum",
				// SummingCombiner.class);
				// SummingCombiner.setCombineAllColumns(scSetting, true);
				// SummingCombiner.setEncodingType(scSetting, SummingCombiner.Type.STRING);
				// to.attachIterator(table, scSetting);
				//
				// scSetting = new IteratorSetting(60, "ageOff", AgeOffFilter.class);
				// AgeOffFilter.setTTL(scSetting, 2500L);
				// to.attachIterator(table, scSetting);
				//
				// scSetting = new IteratorSetting(59, "evenNumber", EvenNumberFilter.class);
				// to.attachIterator(table, scSetting);

				IteratorSetting scSetting = new IteratorSetting(19, "quadSum", QuadraticSummingCombiner.class);
				QuadraticSummingCombiner.setCombineAllColumns(scSetting, true);
				QuadraticSummingCombiner.setLossyness(scSetting, false);

				QuadraticSummingCombiner.testEncoder(encoder, 2);
				to.attachIterator(table, scSetting);

				iters = to.listIterators(table);
				for (Entry<String, EnumSet<IteratorScope>> entry : iters.entrySet()) {
					System.out.println(entry.getKey());
				}
			} catch (TableExistsException | AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
				LOGGER.error(e.getLocalizedMessage());
			}
		}

		AccumuloCrud crud = new DefaultAccumuloCrud(connector, TABLE1, new Authorizations());

		crud.insert(ROW, CF, CQ, new ColumnVisibility(), encoder.encode(1));
		Thread.sleep(1000L);
		crud.insert(ROW, CF, CQ, new ColumnVisibility(), encoder.encode(2));
		Thread.sleep(1000L);
		crud.insert(ROW, CF, CQ, new ColumnVisibility(), encoder.encode(3));
		Thread.sleep(1000L);

		try {
			BatchScanner scanner = connector.createBatchScanner(TABLE1, new Authorizations(), 10);
			scanner.setRanges(Collections.singleton(new Range()));
			for (Entry<Key, Value> entry : scanner) {
				System.out.println(encoder.decode(entry.getValue().get()));
			}
		} catch (TableNotFoundException e) {
			LOGGER.error(e.getLocalizedMessage());
		}

		// crud.insert(ROW, CF, CQ, new ColumnVisibility(), VAL);
		// System.out.println("insert done");
		// Thread.sleep(1000L);
		// Entry<Key, Value> entry = crud.readEntry(ROW, CF, CQ);
		// System.out.println(entry);
		// crud.delete(ROW, CF, CQ);
		// entry = crud.readEntry(ROW, CF, CQ);
		// System.out.println(entry);
		crud.close();

		accumuloClient.close();
	}

}
