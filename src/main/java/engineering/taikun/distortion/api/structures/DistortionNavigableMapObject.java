package engineering.taikun.distortion.api.structures;

import engineering.taikun.distortion.structures.api.DistortionObject;
import org.jetbrains.annotations.Nullable;

import java.util.NavigableMap;
import java.util.function.IntSupplier;

public abstract class DistortionNavigableMapObject<K, V> extends DistortionObject {

	private NavigableMap<K, V> backingMap;
	private short concurrencyLevel;
	private final @Nullable IntSupplier conc_supplier;

	protected DistortionNavigableMapObject(final NavigableMap<K, V> backingMap) {
		this.backingMap = backingMap;
		this.concurrencyLevel = 0;
		this.conc_supplier = null;
	}

	protected DistortionNavigableMapObject(final NavigableMap<K, V> backingMap, final short concurrencyLevel) {
		this.backingMap = backingMap;
		this.concurrencyLevel = concurrencyLevel;
		this.conc_supplier = null;
	}

	protected NavigableMap<K, V> map() {
		return this.backingMap;
	}

	protected short getConcurrencyLevel() {
		if (this.concurrencyLevel == -1) {
			this.concurrencyLevel = (short) this.conc_supplier.getAsInt();
		}

		return this.concurrencyLevel;
	}
}
