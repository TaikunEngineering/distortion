package engineering.taikun.distortion.structures.util;

import engineering.taikun.distortion.structures.api.Struct;

import java.util.Objects;

public abstract class AbstractStruct implements Struct {

	@Override
	public final int hashCode() {
		int toreturn = 1;

		for (int i = 0; i < size(); i++) {
			final Object object = get(i);
			toreturn = 31 * toreturn + (object == null ? 0 : object.hashCode());
		}

		return toreturn;
	}

	@Override
	public final boolean equals(final Object o) {
		if (this == o) {
			return true;
		}

		if (!(o instanceof Struct)) {
			return false;
		}

		final Struct that = (Struct) o;

		final int that_size = that.size();

		if (this.size() != that_size) {
			return false;
		}

		for (int i = 0; i < that_size; i++) {
			if (!Objects.equals(this.get(i), that.get(i))) {
				return false;
			}
		}

		return true;
	}

	@Override
	public String toString() {
		if (size() == 0) {
			return "/\\";
		}

		final StringBuilder sb = new StringBuilder("/");

		for (int i = 0; i < size(); i++) {
			sb.append(get(i));
			sb.append('|');
		}

		sb.setCharAt(sb.length() - 1, '\\');

		return sb.toString();
	}
}
