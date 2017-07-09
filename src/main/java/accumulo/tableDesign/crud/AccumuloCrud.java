package accumulo.tableDesign.crud;

import java.util.List;

import accumulo.tableDesign.data.User;

public interface AccumuloCrud {

	public void insert(User user);

	public void showWholeTable(String tableName);

	public List<String> queryExact(String fieldName, String fieldValue);

	public List<String> queryPrefix(String fieldName, String fieldValue);

	public List<String> querySuffix(String fieldName, String fieldValue);

	public List<String> querySubString(String fieldName, String fieldValue);

}
