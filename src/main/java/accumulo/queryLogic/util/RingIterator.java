package accumulo.queryLogic.util;

import java.util.Iterator;
import java.util.List;

public class RingIterator<E> implements Iterator<E> {

	final private int listSize;
	private List<E> list;
	private int currentPos;

	public RingIterator(List<E> list) {
		this.list = list;
		listSize = list.size();
		currentPos = 0;
	}

	@Override
	public boolean hasNext() {
		if (list == null || listSize == 0) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public E next() {
		if (currentPos < listSize - 1) {
			return list.get(currentPos++);
		} else {
			return list.get(0);
		}
	}

	public List<E> getList() {
		return list;
	}

}
