package engineering.taikun.stitch;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

public class StitchTest {

	public static void main(final String[] args) throws Exception {

		String test = new String(Files.readAllBytes(Paths.get("src/test/java/engineering/taikun/stitch/test.txt")));

		HashMap<String, Object> values = new HashMap<>();
		values.put("debug", false);

		System.out.println(Stitch.evaluate(test, values));

	}

}
