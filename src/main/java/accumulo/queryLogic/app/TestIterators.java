package accumulo.queryLogic.app;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import accumulo.documentPartioned.crud.AccumuloCrud;
import accumulo.documentPartioned.crud.DefaultAccumuloCrud;
import accumulo.queryLogic.iterators.ExactTermIterator;
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
				User harry = new User(USERNAME + i);
				harry.setAge(i % 20);
				harry.setCity(CITY);
				crud.insert(harry);
			}

			// crud.showWholeTable(DATA_TABLE);
			crud.showWholeTable(INDEX_TABLE);

			Scanner scanner = connector.createScanner(INDEX_TABLE, Authorizations.EMPTY);
			Map<String, String> options = new HashMap<>(2);
			options.put("fieldName", "city");
			options.put("fieldValue", "Hogwarts");
			IteratorSetting setting = new IteratorSetting(70, ExactTermIterator.class, options);
			scanner.addScanIterator(setting);
			System.out.println("--------------------");
			for (Entry<Key, Value> entry : scanner) {
				System.out.println(entry);
			}
			crud.close();

		} catch (

		Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}

}
