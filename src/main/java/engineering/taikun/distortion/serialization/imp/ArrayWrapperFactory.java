package engineering.taikun.distortion.serialization.imp;

import engineering.taikun.distortion.serialization.api.ByteArray;
import engineering.taikun.distortion.serialization.api.ByteArrayFactory;

/**
 * A simple factory for {@link ArrayWrapper} implementations of {@link ByteArray}
 */
public class ArrayWrapperFactory implements ByteArrayFactory<ArrayWrapper> {

	@Override
	public ArrayWrapper allocate(final int length) {
		return new ArrayWrapper(new byte[length]);
	}

}
