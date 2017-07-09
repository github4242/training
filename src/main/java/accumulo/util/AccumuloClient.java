package accumulo.util;

import java.io.Closeable;

import org.apache.accumulo.core.client.Connector;

public interface AccumuloClient extends Closeable {

	public Connector getConnector();

}
