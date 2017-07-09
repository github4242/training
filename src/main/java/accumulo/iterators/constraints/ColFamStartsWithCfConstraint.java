package accumulo.iterators.constraints;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;

public class ColFamStartsWithCfConstraint implements Constraint {

	private static final byte[] CF = "cf".getBytes();
	private static final short CF_VIOLATION = 1;

	@Override
	public String getViolationDescription(short violationCode) {
		switch (violationCode) {
		case 1:
			return "ColumFamily does not start with 'cf'";
		}
		return null;
	}

	@Override
	public List<Short> check(Environment env, Mutation mutation) {
		List<Short> violations = null;
		for (ColumnUpdate update : mutation.getUpdates()) {
			boolean rightPrefix = startsWith(update.getColumnFamily(), 0, CF);
			if (!rightPrefix) {
				if (violations == null) {
					violations = new ArrayList<>();
				}
				violations.add(CF_VIOLATION);
			}
		}
		return violations == null ? null : violations;
	}

	private static boolean startsWith(byte[] source, int offset, byte[] match) {

		if (match.length > (source.length - offset)) {
			return false;
		}

		for (int i = 0; i < match.length; i++) {
			if (source[offset + i] != match[i]) {
				return false;
			}
		}
		return true;
	}

}
