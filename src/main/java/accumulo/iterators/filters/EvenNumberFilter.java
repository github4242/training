package accumulo.iterators.filters;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;

public class EvenNumberFilter extends Filter {

	@Override
	public boolean accept(Key k, Value v) {
		int val = Integer.parseInt(new String(v.get()));
		return val % 2 == 0;
	}

}
