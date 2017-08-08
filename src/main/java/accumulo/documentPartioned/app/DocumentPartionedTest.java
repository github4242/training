package accumulo.documentPartioned.app;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.Connector;

import accumulo.documentPartioned.crud.AccumuloCrud;
import accumulo.documentPartioned.crud.DefaultAccumuloCrud;
import accumulo.tableDesign.data.User;
import accumulo.tableDesign.init.AccumuloTablesCreation;
import accumulo.util.AccumuloClient;
import accumulo.util.MiniAccumuloClient;

public class DocumentPartionedTest {

	private static final String PASSWORD = "pass";
	private static final String DATA_TABLE = "user";
	private static final String INDEX_TABLE = "user_index";
	private static final Collection<String> TABLES = Arrays.asList(DATA_TABLE, INDEX_TABLE);
	private static final String USERNAME = "HarryPotter";
	private static final String CITY = "Hogwarts";

	public static void main(String[] args) {

		try (AccumuloClient client = new MiniAccumuloClient(PASSWORD)) {
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
			// crud.showWholeTable(INDEX_TABLE);

			List<String> results = crud.queryExact("name", "HarryPotter0");
			// List<Pair<String, String>> fields = Arrays.<Pair<String, String>>asList(new
			// Pair<>("age", "2"),
			// new Pair<>("name", "HarryPotter3"));
			// List<String> results = crud.queryMultiExact(fields);
			int i = 0;
			for (String res : results) {
				System.out.println(res);
				i++;
			}
			System.out.println(i);

			crud.close();

		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

}
