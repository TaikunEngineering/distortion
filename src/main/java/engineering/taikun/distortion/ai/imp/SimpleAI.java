package engineering.taikun.distortion.ai.imp;

import engineering.taikun.distortion.ai.api.DistortionAI;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;

/*
 * This is a simple (ish) implementation of a Distortion AI system. It assumes identifiers only block on themselves,
 * which seems like a safe mostly-truth.
 *
 * It operates by storing 2 + N values per Identifier in a map
 *
 * 1 value is for block tendency (or lack thereof). A low value overrides and will predict optimistically.
 *
 * 1 value is for stall tendency. A high value override and will predict for total-stall.
 *
 * N values are for block tendency for each string in the identifier array.
 *
 * Non-collisions have their values reduced since it's seen as very strong evidence of lack of issue.
 *
 * Collisions have their values buffed slightly since collisions can be from anything.
 *
 * Stalls are collisions without any common strings in the state.
 *
 * Data structure wise, this implementation uses a ConcurrentHashMap. Keys are clones and the values contain
 * weak references to the passed Identifier. References are updated if an operation uses an identifier that's equal
 * but not the identity match.
 *
 * Generally, the identifier should be cached if it's in long term use. Use of a new one is perceived as an indicator
 * that the map should "change ownership".
 *
 * Since culling is shifted over to the map to handle, it's simply done by keeping a last-touched timestamp. Anything
 * over a minute stale is dropped.
 *
 * Culling is done at the end of #train if an entry is added to the map.
 *
 * Also, all requests have a 2% to be optimistic regardless of AI info since running optimistically is the only way to
 * train.
 *
 * 'simple'
 *
 * Note: This makes use of 'lazy' synchronization of data. A few updates might be lost when threads diverge, but
 * construction and insertion is enforced by the CHM.
 */

/**
 * <p>A basic AI that treats the Identifier hierarchy as a parent-child tree</p>
 *
 * <p>It will learn blocking behavior relative to the universe, self and parents (including children)</p>
 *
 * <p>That is, ["fruit", "banana"] will 1. profile  generically, determining if that identifier should always be
 * executed optimistically or pessimistically, and 2. profile against ["fruit", "banana", *] and ["fruit", *]</p>
 *
 * <p>To emulate tracking third parties, you should identify your functions with nodes from every execution tree you
 * care about. For example, if you wanted to track users in an MMORPG, you would identify every function with at least
 * the player and the in-game location.</p>
 */
public class SimpleAI implements DistortionAI {

	// parameters determined by simulation with exponentially-hot data with high (32) CPU count
	// should offer ok performance without major performance cliffs

	public final int CHAOS_DENOMINATOR;           // 50
	public final double GLOBAL_BLOCK_OPTIMISM;    // 0.250
	public final double GLOBAL_BLOCK_STALL;       // 0.655
	public final double FINE_BLOCK_STALL;         // 0.569

	public final double BLOCK_TRAINING_FOR;       // 0.401
	public final double BLOCK_TRAINING_AGAINST;   // 0.884

	public final double STALL_TRAINING_FOR;       // 0.112
	public final double STALL_TRAINING_AGAINST;   // 0.333

	public final double FINE_TRAINING_FOR;        // 0.643
	public final double FINE_TRAINING_AGAINST;    // 0.669

	private final ConcurrentHashMap<Identifier, SimpleAIValue> map = new ConcurrentHashMap<>();
	private final Random random = new Random();

	public SimpleAI() {
		this.CHAOS_DENOMINATOR = 50;
		this.GLOBAL_BLOCK_OPTIMISM = 0.250;
		this.GLOBAL_BLOCK_STALL = 0.655;
		this.FINE_BLOCK_STALL = 0.569;

		this.BLOCK_TRAINING_FOR = 0.401;
		this.BLOCK_TRAINING_AGAINST = 0.884;

		this.STALL_TRAINING_FOR = 0.112;
		this.STALL_TRAINING_AGAINST = 0.333;

		this.FINE_TRAINING_FOR = 0.643;
		this.FINE_TRAINING_AGAINST = 0.669;
	}

	public SimpleAI(final double[] tuning_parameters) {
		if (tuning_parameters.length < 10)
			throw new IllegalArgumentException();

		final int chaos = (int) tuning_parameters[0];

		if (chaos < 1)
			throw new IllegalArgumentException();

		this.CHAOS_DENOMINATOR = chaos;

		for (int i = 1; i < 10; i++) {
			if (tuning_parameters[i] < 0 || tuning_parameters[i] > 1)
				throw new IllegalArgumentException();
		}

		this.GLOBAL_BLOCK_OPTIMISM = tuning_parameters[1];
		this.GLOBAL_BLOCK_STALL = tuning_parameters[2];
		this.FINE_BLOCK_STALL = tuning_parameters[3];

		this.BLOCK_TRAINING_FOR = tuning_parameters[4];
		this.BLOCK_TRAINING_AGAINST = tuning_parameters[5];

		this.STALL_TRAINING_FOR = tuning_parameters[6];
		this.STALL_TRAINING_AGAINST = tuning_parameters[7];

		this.FINE_TRAINING_FOR = tuning_parameters[8];
		this.FINE_TRAINING_AGAINST = tuning_parameters[9];
	}

	@Override
	public void train(
			final Collection<Collection<Identifier>> state, final Collection<Identifier> identifiers,
			final int mode
	) {
		boolean modified_map = false;
		// collided means that there was something in the state that matched an identifier
		boolean collided_OR_ten = mode == 10;

		for (final Identifier identifier : identifiers) {
			if (!this.map.containsKey(identifier)) {
				modified_map = true;
			}

			final SimpleAIValue value = this.map.computeIfAbsent(
					identifier.copy(), o -> new SimpleAIValue(identifier)
			);

			// potentially inefficient, yes, but it (easily) avoids the map from tearing.
			value.updateReference(identifier);
			value.touch();

			// mode 10 is effectively a special case
			if (mode == 10) {
				value.updateGlobalStall(true);
			} else {

				final boolean[] present = new boolean[identifier.strings.length];
			big:
				for (final Collection<Identifier> collection : state) {
					for (final Identifier state_identifier : collection) {

						for (int i = 0; i < identifier.strings.length; i++) {
							if (state_identifier.strings.length > i && state_identifier.strings[i].equals(identifier.strings[i])) {
								present[i] = true;

								if (i == identifier.strings.length - 1) {
									break big;
								}
							}
						}

					}
				}

				switch (mode) {
					case 0: {
						value.updateGlobalBlock(false);
						value.updateGlobalStall(false);
						for (int i = 0; i < present.length; i++) {
							if (present[i]) {
								value.updateFineBlock(false, i);
								collided_OR_ten = true;
							}
						}
						break;
					}
					case 20:
					case 30:
					case 40: {
						value.updateGlobalBlock(true);
						for (int i = 0; i < present.length; i++) {
							if (present[i]) {
								value.updateFineBlock(true, i);
								collided_OR_ten = true;
							}
						}
						break;
					}
					default:
						throw new RuntimeException("Unknown mode: " + mode);
				}

			}

		}

		// if there was a bad and no collision, assume stall

		// note: we test if the state was empty. If the state was empty and the mode != 0, we simply don't know what
		// happened. State should have contained something, but we don't know what. Guessing what it should have contained
		// is dubious, so we do nothing in this particular error-case.

		if (mode != 0 && !collided_OR_ten && !state.isEmpty()) {

			for (final Identifier identifier : identifiers) {
				if (!this.map.containsKey(identifier)) {
					modified_map = true;
				}

				final SimpleAIValue value = this.map.computeIfAbsent(
						identifier.copy(), o -> new SimpleAIValue(identifier)
				);

				value.updateGlobalStall(true);
			}

		}

		if (modified_map) {
			for (final Iterator<SimpleAIValue> it = this.map.values().iterator(); it.hasNext();) {
				if (it.next().isRemovable()) {
					it.remove();
				}
			}
		}
	}

	@Override
	public ExecutionMode predict(
			final Collection<Collection<Identifier>> state, final Collection<Identifier> identifiers,
			final boolean[] flags
	) {
		// >best predictor
		if (this.random.nextInt(this.CHAOS_DENOMINATOR) == 0) {
			return ExecutionMode.OPTIMISTIC;
		}

		final SimpleAIValue[] value_cache = new SimpleAIValue[identifiers.size()];

		// fast check, short-circuiting
		{
			int i = 0;
			for (final Identifier identifier : identifiers) {
				final SimpleAIValue value = this.map.get(identifier);

				value_cache[i] = value;

				i++;

				if (value == null) {
					continue;
				}

				// tend to be optimistic, so <= then >. In reality, this doesn't matter

				if (value.global_block_tendency <= this.GLOBAL_BLOCK_OPTIMISM) {
					return ExecutionMode.OPTIMISTIC;
				}

				if (value.global_stall_tendency > this.GLOBAL_BLOCK_STALL) {
					return ExecutionMode.GLOBAL_STALL;
				}
			}
		}

		// slow check, it is what it is
		boolean tainted = false;
		int i = 0;
	stateloop:
		for (final Collection<Identifier> state_collection : state) {
			for (final Identifier state_identifier : state_collection) {

				int j = 0;
				for (final Identifier identifier : identifiers) {

					final SimpleAIValue value = value_cache[j];

					if (value == null) {
						continue;
					}

					for (int x = 0; x < state_identifier.strings.length && x < identifier.strings.length; x++) {
						if (state_identifier.strings[x].equals(identifier.strings[x])) {

							if (Double.longBitsToDouble(value.fine_block_tendency.get(x)) > this.FINE_BLOCK_STALL) {
								flags[i] = true;
								tainted = true;
								continue stateloop;
							}
						}
					}

					j++;
				}
			}
			i++;
		}

		if (tainted) {
			return ExecutionMode.FINE_BLOCK;
		} else {
			return ExecutionMode.OPTIMISTIC;
		}
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();

		sb.append("{\n");

		for (final Entry<Identifier, SimpleAIValue> entry : this.map.entrySet()) {
			sb.append("  ").append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
		}

		sb.append('}');

		return sb.toString();
	}

	@SuppressWarnings("NonAtomicOperationOnVolatileField")
	public class SimpleAIValue {

		// volatile is good enough for the stuff here

		// ...except for arrays, where we have to use the atomic array and convert bits

		public volatile WeakReference<Identifier> reference;
		public volatile long touched;

		public volatile double global_block_tendency;
		public volatile double global_stall_tendency;
		public final AtomicLongArray fine_block_tendency;

		public SimpleAIValue(final Identifier source) {
			this.reference = new WeakReference<>(source);
			this.touched = System.currentTimeMillis();

			this.global_block_tendency = 0;
			this.global_stall_tendency = 0;
			this.fine_block_tendency = new AtomicLongArray(source.strings.length);
		}

		public void updateReference(final Identifier source) {
			// Assume source equals copy
			if (this.reference.get() != source) {
				this.reference = new WeakReference<>(source);
			}
		}

		public boolean isRemovable() {
			return this.reference.get() == null || this.touched < System.currentTimeMillis() - 60 * 1000L;
		}

		public void touch() {
			this.touched = System.currentTimeMillis();
		}

		public void updateGlobalBlock(final boolean blocked) {
			if (blocked) {
				this.global_block_tendency = this.global_block_tendency * SimpleAI.this.BLOCK_TRAINING_FOR + (1 - SimpleAI.this.BLOCK_TRAINING_FOR);
			} else {
				this.global_block_tendency *= SimpleAI.this.BLOCK_TRAINING_AGAINST;
			}
		}

		public void updateGlobalStall(final boolean stalled) {
			if (stalled) {
				this.global_stall_tendency = this.global_stall_tendency * SimpleAI.this.STALL_TRAINING_FOR + (1 - SimpleAI.this.STALL_TRAINING_FOR);
			} else {
				this.global_stall_tendency *= SimpleAI.this.STALL_TRAINING_AGAINST;
			}
		}

		public void updateFineBlock(final boolean blocked, final int index) {
			if (blocked) {
				this.fine_block_tendency.updateAndGet(
						index, operand -> Double.doubleToRawLongBits(Double.longBitsToDouble(operand) * SimpleAI.this.FINE_TRAINING_FOR + (1 - SimpleAI.this.FINE_TRAINING_FOR))
				);
			} else {
				this.fine_block_tendency.updateAndGet(
						index, operand -> Double.doubleToRawLongBits(Double.longBitsToDouble(operand) * SimpleAI.this.FINE_TRAINING_AGAINST)
				);
			}
		}

		@Override
		public String toString() {
			final double[] fine = new double[this.fine_block_tendency.length()];

			for (int i = 0; i < fine.length; i++) {
				fine[i] = Double.longBitsToDouble(this.fine_block_tendency.get(i));
			}

			return "Block: " + this.global_block_tendency + " Stall: " + this.global_stall_tendency + " Fine: "
					+ Arrays.toString(fine);
		}
	}

}
