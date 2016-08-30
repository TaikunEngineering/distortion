package engineering.taikun.distortion.api.fn;

import engineering.taikun.distortion.ai.api.DistortionAI.Identifier;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

/**
 * <p>A SAM class for efficiently scheduling functions that are forced to be (initially) scheduled optimistically</p>
 *
 * <p>This function won't invoke the AI for a scheduling prediction, nor will its execution train the AI</p>
 */
public abstract class OptimisticFunction extends AdvancedFunction {
	@Override
	@Nullable public Collection<Identifier> getIdentifiers() {
		return null;
	}

	@Override
	public ExecutionPolicy getExecutionPolicy() {
		return ExecutionPolicy.FORCE_OPTIMISTIC;
	}
}
