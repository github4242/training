package accumulo.tableDesign.init;

import java.util.Collection;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.TableOperations;

public class AccumuloTablesCreation {

	private Connector connector;

	public AccumuloTablesCreation(Connector connector) {
		this.connector = connector;
	}

	public void create(Collection<String> tables) {
		TableOperations tops = connector.tableOperations();
		for (String table : tables) {
			try {
				tops.create(table);
			} catch (TableExistsException e) {
				System.out.println("Table ' " + table + " already exists. Skip this one.");
			} catch (AccumuloSecurityException e) {
				System.out.println("User does not have permissions for table creation.");
			} catch (AccumuloException e) {
				throw new RuntimeException(e);
			}
		}
	}

}
