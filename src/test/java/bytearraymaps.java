import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by Phil on 9/18/2014.
 */
public class bytearraymaps {

	public static void main(String[] args) {

		HashMap<byte[], byte[]> test = new HashMap<>();

		test.put(new byte[]{ 0, 0}, new byte[]{ 34, 69 });

		System.out.println(Arrays.toString(test.get(new byte[]{ 0, 0 })));

		System.out.println(test);



	}

}
