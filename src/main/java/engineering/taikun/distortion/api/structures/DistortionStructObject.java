package engineering.taikun.distortion.api.structures;

import engineering.taikun.distortion.structures.api.Struct;
import engineering.taikun.distortion.structures.api.DistortionObject;

public abstract class DistortionStructObject extends DistortionObject {

	private Struct backingStruct;

	protected DistortionStructObject(final Struct backingStruct) {
		this.backingStruct = backingStruct;
	}

	protected Struct struct() {
		return this.backingStruct;
	}
}
