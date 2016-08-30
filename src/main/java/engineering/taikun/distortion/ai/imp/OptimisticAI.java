package engineering.taikun.distortion.ai.imp;

import engineering.taikun.distortion.ai.api.DistortionAI;

import java.util.Collection;

public class OptimisticAI implements DistortionAI {
	@Override public ExecutionMode predict(
			final Collection<Collection<Identifier>> state, final Collection<Identifier> identifiers, final boolean[] flags
	) {
		return ExecutionMode.OPTIMISTIC;
	}
}
