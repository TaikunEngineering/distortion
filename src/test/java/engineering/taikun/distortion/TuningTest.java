package engineering.taikun.distortion;

import engineering.taikun.distortion.SimpleAITuning.SimpleAIParameters;

public class TuningTest {

	public static void main(String[] args) {

		SimpleAIParameters params = new SimpleAIParameters();

//		1.703
//
//		params.params[1] = 0.2;
//		params.params[2] = 0.5;
//		params.params[3] = 0.2;
//
//		params.params[4] = 0.9;
//		params.params[5] = 0.95;
//
//		params.params[6] = 0.9;
//		params.params[7] = 0.95;
//
//		params.params[8] = 0.9;
//		params.params[9] = 0.95;

//		1.693
//
//		params.params[1] = 0.5;
//		params.params[2] = 0.5;
//		params.params[3] = 0.5;
//
//		params.params[4] = 0.5;
//		params.params[5] = 0.5;
//
//		params.params[6] = 0.5;
//		params.params[7] = 0.5;
//
//		params.params[8] = 0.5;
//		params.params[9] = 0.5;

//		1.702

		params.params[1] = 0.17;
		params.params[2] = 0.65;
		params.params[3] = 0.57;

		params.params[4] = 0.43;
		params.params[5] = 0.88;

		params.params[6] = 0.43;
		params.params[7] = 0.34;

		params.params[8] = 0.64;
		params.params[9] = 0.68;

		System.out.println(SimpleAITuning.evaluate(params));
		System.out.println(SimpleAITuning.evaluate(params));
		System.out.println(SimpleAITuning.evaluate(params));
		System.out.println(SimpleAITuning.evaluate(params));
		System.out.println(SimpleAITuning.evaluate(params));

	}

}
