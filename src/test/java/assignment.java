public class assignment {

	static int x = 0;

	static int f()
	{
		x = x + 10;
		return 1;
	}

	public static void main(String[] args)
	{
		x += f();
		System.out.println(x);
	}

}
