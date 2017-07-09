package accumulo.util;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

@SuppressWarnings("deprecation")
public class MockAccumuloClient implements AccumuloClient {

	private Connector connector;

	public MockAccumuloClient() {
		Instance accumulo = new MockInstance();
		try {
			connector = accumulo.getConnector("root", new PasswordToken(""));
		} catch (AccumuloException | AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public Connector getConnector() {
		return connector;
	}

}
