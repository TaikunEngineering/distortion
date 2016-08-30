package engineering.taikun.distortion.structures.api;

public abstract class DistortionObject {

	/**
	 * <p>TO BE CALLED ONLY BY DISTORTION</p>
	 *
	 * <p>Called after object instantiation so that transients may be initialized</p>
	 *
	 * <p>Treat this method as if it were in a constuctor. Be careful when passing 'this' around and note that other
	 * threads will likely see incomplete object state.</p>
	 */
	public void distortionInit() {}
}
