package engineering.taikun.distortion.api.fn;

import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;

/**
 * <p>An abstract class which allows you to easily compose more advanced {@link DistortionFunction}s</p>
 *
 * <p>This class adds two methods: {@link #branch} and {@link #doProtected}. These methods are to be executed within
 * your {@link #transform} method so that you can dynamically schedule branches and a protected block.</p>
 */
public abstract class AdvancedFunction implements DistortionFunction {

	private ArrayList<DistortionFunction> branches = null;
	private @Nullable DistortionFunction protectedF = null;

	/**
	 * <p>Specify a {@link DistortionFunction} that will be executed in the timespace immediately after this
	 * function and after any protected functions it spawns</p>
	 *
	 * <p>Multiple functions can be branched by the same DistortionFunction</p>
	 *
	 * <p>Warning: Branching functions while as a 'protected' function will <b>NOT</b> yield protected branches. If this
	 * was something you wanted to emulate, just run the code normally in the protected function.</p>
	 *
	 * @param function {@link DistortionFunction} to run as a branch
	 */
	protected final void branch(final DistortionFunction function) {
		if (this.branches == null)
			this.branches = new ArrayList<>();
		this.branches.add(function);
	}

	/**
	 * <p>Specify a {@link DistortionFunction} that is protected (guaranteed to run exactly once) and that will be
	 * executed in the timespace immediately after this function</p>
	 *
	 * <p>Note: Only one function can be specified (because two serial functions can be written as one bigger one)</p>
	 *
	 * @param function {@link DistortionFunction} to execute in a protected environment
	 * @throws IllegalArgumentException Invoking this twice in a {@link #transform} call is an error, compose your
	 * functions into one bigger function
	 */
	protected final void doProtected(final DistortionFunction function) {
		if (this.protectedF != null)
			throw new IllegalStateException("Only one protected function can be defined");
		this.protectedF = function;
	}

	@Override
	public final ArrayList<DistortionFunction> getBranches() {
		return this.branches;
	}

	@Override
	public final DistortionFunction getProtectedFunction() {
		return this.protectedF;
	}

	@Override
	public final void reset() {
		if (this.branches != null)
			this.branches.clear();
		this.protectedF = null;
	}

}
