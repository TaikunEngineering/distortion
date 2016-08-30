package engineering.taikun.distortion;

import java.util.concurrent.ExecutionException;

public class BigDistortionTestLooper {

	public static void main(final String[] args) throws ExecutionException, InterruptedException {
		while(true) {
			BigDistortionTest.main();
		}
	}

}
