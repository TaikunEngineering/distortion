package engineering.taikun.distortion.api.structures;

import engineering.taikun.distortion.structures.api.DistortionObject;
import org.jetbrains.annotations.Nullable;

import java.util.NavigableSet;
import java.util.function.IntSupplier;

public abstract class DistortionNavigableSetObject<T> extends DistortionObject {

	private NavigableSet<T> backingSet;
	private short concurrencyLevel;
	private final @Nullable IntSupplier conc_supplier;

	protected DistortionNavigableSetObject(final NavigableSet<T> backingSet) {
		this.backingSet = backingSet;
		this.concurrencyLevel = 0;
		this.conc_supplier = null;
	}

	protected DistortionNavigableSetObject(final NavigableSet<T> backingSet, final short concurrencyLevel) {
		this.backingSet = backingSet;
		this.concurrencyLevel = concurrencyLevel;
		this.conc_supplier = null;
	}

	protected NavigableSet<T> set() {
		return this.backingSet;
	}

	protected short getConcurrencyLevel() {
		if (this.concurrencyLevel == -1) {
			this.concurrencyLevel = (short) this.conc_supplier.getAsInt();
		}

		return this.concurrencyLevel;
	}
}
