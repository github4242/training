package accumulo.queryLogic.util;

import java.io.IOException;

import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;

public class ProxyEnv implements IteratorEnvironment {

	private IteratorEnvironment env;

	public ProxyEnv(IteratorEnvironment env) {
		this.env = env;
	}

	@Override
	public SortedKeyValueIterator<Key, Value> reserveMapFileReader(String mapFileName) throws IOException {
		return env.reserveMapFileReader(mapFileName);
	}

	@Override
	public AccumuloConfiguration getConfig() {
		return env.getConfig();
	}

	@Override
	public IteratorScope getIteratorScope() {
		return env.getIteratorScope();
	}

	@Override
	public boolean isFullMajorCompaction() {
		return env.isFullMajorCompaction();
	}

	@Override
	public void registerSideChannel(SortedKeyValueIterator<Key, Value> iter) {
		env.registerSideChannel(iter);
	}

	@Override
	public Authorizations getAuthorizations() {
		return env.getAuthorizations();
	}

	@Override
	public IteratorEnvironment cloneWithSamplingEnabled() {
		return env.cloneWithSamplingEnabled();
	}

	@Override
	public boolean isSamplingEnabled() {
		return false;
	}

	@Override
	public SamplerConfiguration getSamplerConfiguration() {
		return env.getSamplerConfiguration();
	}

}
