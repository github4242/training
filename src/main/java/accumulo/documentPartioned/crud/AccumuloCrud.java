package accumulo.documentPartioned.crud;

import java.io.Closeable;
import java.util.List;

import org.apache.commons.math3.util.Pair;

import accumulo.tableDesign.data.User;

public interface AccumuloCrud extends Closeable {

	public void insert(User user);

	public void showWholeTable(String tableName);

	public List<String> queryExact(String fieldName, String fieldValue);

	public List<String> queryPrefix(String fieldName, String fieldValue);

	public List<String> querySuffix(String fieldName, String fieldValue);

	public List<String> querySubString(String fieldName, String fieldValue);

	public List<String> queryMultiExact(List<Pair<String, String>> fields);

}
