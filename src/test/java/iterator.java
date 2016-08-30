import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class iterator {

	public static void main(String[] args) {

		List list = Arrays.asList(4, 4,5 ,56, 2,4, 2,4, 2,4, 25, 6);

		System.out.println(list.size());

		int count = 0;
		for (Iterator it = list.iterator(); it.hasNext(); it.next()) {
			count++;
		}
		System.out.println(count);


	}

}
