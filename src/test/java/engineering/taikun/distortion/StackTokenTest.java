package engineering.taikun.distortion;

import engineering.taikun.distortion.store.imp.StackToken;

import java.util.ArrayList;
import java.util.Collections;

public class StackTokenTest {

	public static void main(String[] args) {

		ArrayList<StackToken> tokens = new ArrayList<>();

		StackToken token1 = new StackToken();

		tokens.add(token1);

		StackToken token2 = token1.getNextToken();

		tokens.add(token2);

		StackToken token3 = token2.getNextToken();

		tokens.add(token3);

		StackToken token11 = token1.getDeepToken();

		tokens.add(token11);

		StackToken token21 = token2.getDeepToken();

		tokens.add(token21);

		StackToken token12 = token11.getNextToken();

		tokens.add(token12);

		StackToken token13 = token12.getNextToken();

		tokens.add(token13);

		StackToken token121 = token12.getDeepToken();

		tokens.add(token121);

		System.out.println(tokens);

		Collections.sort(tokens);

		System.out.println(tokens);


	}

}
