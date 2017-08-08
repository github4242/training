package accumulo.queryLogic.app;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.net.util.Base64;

import accumulo.documentPartioned.crud.AccumuloCrud;
import accumulo.documentPartioned.crud.DefaultAccumuloCrud;
import accumulo.queryLogic.iterators.AndIterator;
import accumulo.queryLogic.iterators.ExactTermIterator;
import accumulo.queryLogic.iterators.QueryIterator;
import accumulo.tableDesign.data.User;
import accumulo.tableDesign.init.AccumuloTablesCreation;
import accumulo.util.AccumuloClient;
import accumulo.util.MockAccumuloClient;

public class TestIterators {

	private static final String DATA_TABLE = "user";
	private static final String INDEX_TABLE = "user_index";
	private static final Collection<String> TABLES = Arrays.asList(DATA_TABLE, INDEX_TABLE);
	private static final String USERNAME = "HarryPotter";
	private static final String CITY = "Hogwarts";

	public static void main(String[] args) {

		try (AccumuloClient client = new MockAccumuloClient()) { // new MiniAccumuloClient(PASSWORD)) {
			Connector connector = client.getConnector();
			AccumuloTablesCreation tc = new AccumuloTablesCreation(connector);
			tc.create(TABLES);
			AccumuloCrud crud = new DefaultAccumuloCrud(connector, DATA_TABLE, INDEX_TABLE);

			for (int i = 0; i < 5; i++) {
				User harry = new User(USERNAME + i % 3);
				harry.setAge(i % 20);
				harry.setCity(CITY);
				crud.insert(harry);
			}

			// crud.showWholeTable(DATA_TABLE);
			crud.showWholeTable(INDEX_TABLE);

			Scanner scanner = connector.createScanner(INDEX_TABLE, Authorizations.EMPTY);
			ExactTermIterator iter = new ExactTermIterator("name", "HarryPotter0");
			IteratorSetting setting = iter.getIteratorSetting(70);
			scanner.addScanIterator(setting);
			System.out.println("--------------------");
			for (Entry<Key, Value> entry : scanner) {
				System.out.println(entry);
			}
			scanner.clearScanIterators();

			String serIter = iter.serialize();

			ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decodeBase64(serIter));
			DataInputStream input = new DataInputStream(bis);
			String className = input.readUTF();
			String fieldName = input.readUTF();
			String fieldValue = input.readUTF();
			QueryIterator iter2 = (QueryIterator) crud.getClass().getClassLoader().loadClass(className)
					.getConstructor(String.class, String.class).newInstance(fieldName, fieldValue);

			IteratorSetting setting2 = iter2.getIteratorSetting(70);
			scanner.addScanIterator(setting2);
			System.out.println("--------------------");
			for (Entry<Key, Value> entry : scanner) {
				System.out.println(entry);
			}
			scanner.clearScanIterators();

			ExactTermIterator iter4 = new ExactTermIterator("city", "Hogwarts");
			ExactTermIterator iter5 = new ExactTermIterator("age", "3");
			List<QueryIterator> terms = Arrays.asList(iter, iter4, iter5);
			AndIterator andIter = new AndIterator(terms);
			IteratorSetting setting3 = andIter.getIteratorSetting(70);
			scanner.addScanIterator(setting3);
			System.out.println("--------------------");

			// Iterator<Entry<Key, Value>> scanIter = scanner.iterator();
			// while (scanIter.hasNext()) {
			// System.out.println(scanIter.hasNext() + " -- " + scanIter.next() + "---" +
			// scanIter.hasNext());
			// }

			for (Entry<Key, Value> entry : scanner) {
				System.out.println(entry);
			}
			scanner.clearScanIterators();

			crud.close();

		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}

}
