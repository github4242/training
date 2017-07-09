package accumulo.tableDesign.data;

import org.json.JSONObject;

public class User {

	private String name;
	private int age;
	private String city;

	public User(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	@Override
	public String toString() {
		JSONObject json = new JSONObject(this);
		return json.toString(2);
	}

}
