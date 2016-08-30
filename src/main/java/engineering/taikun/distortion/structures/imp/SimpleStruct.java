package engineering.taikun.distortion.structures.imp;

import engineering.taikun.distortion.structures.util.AbstractStruct;

public class SimpleStruct extends AbstractStruct {

	public final Object[] data;

	public SimpleStruct(final int size) {
		if (size > 250) {
			throw new IllegalArgumentException("Size is capped at 250");
		}

		this.data = new Object[size];
	}

	@Override
	public void set(final int index, final Object value) {
		this.data[index] = value;
	}

	@Override
	public Object get(final int index) {
		return this.data[index];
	}

	@Override
	public int size() {
		return this.data.length;
	}
}
