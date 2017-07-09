package accumulo.documentPartioned.iterators;

import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;

public class RegexCheckIterator extends Filter {

	private static final String REGEX_TERM = "regex";
	private String regex;

	@Override
	public boolean accept(Key k, Value v) {
		return v.toString().matches(regex);
	}

	@Override
	public boolean validateOptions(Map<String, String> options) {
		if (!super.validateOptions(options) || !options.containsKey(REGEX_TERM)) {
			throw new IllegalArgumentException("Option \"regex\" is mandatory.");
		}
		regex = options.get(REGEX_TERM);
		return true;
	}

	@Override
	public IteratorOptions describeOptions() {
		IteratorOptions options = super.describeOptions();
		options.addNamedOption(REGEX_TERM, "Regex that is tested.");
		return options;
	}

}
