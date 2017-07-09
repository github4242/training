package accumulo.util;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class MiniAccumuloClient implements AccumuloClient {

	private static Logger LOGGER = LogManager.getLogger(MiniAccumuloClient.class);

	private MiniAccumuloCluster accumulo;
	private File tempDir;
	private String password;
	private Connector connector;

	public MiniAccumuloClient(String password) {
		this.password = password;
	}

	@Override
	public void close() throws IOException {
		try {
			accumulo.stop();
			if (tempDir.exists()) {
				FileUtils.forceDelete(tempDir);
			}
		} catch (InterruptedException e) {
			LOGGER.error(e.getLocalizedMessage());
			throw new RuntimeException(e);
		}
	}

	@Override
	public Connector getConnector() {
		if (connector == null) {
			initializeMiniAccumuloCluster();
		}
		return connector;
	}

	private synchronized void initializeMiniAccumuloCluster() {
		if (connector == null) {
			try {
				tempDir = new File("/tmp/MiniAccumuloCluster");
				if (tempDir.exists()) {
					FileUtils.forceDelete(tempDir);
				}
				FileUtils.forceMkdir(tempDir);
				accumulo = new MiniAccumuloCluster(tempDir, password);
				accumulo.start();
				Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
				connector = instance.getConnector("root", new PasswordToken(password));
			} catch (IOException | InterruptedException | AccumuloSecurityException | AccumuloException e) {
				LOGGER.error(e.getLocalizedMessage());
				throw new RuntimeException(e);
			}
		}
	}

}
