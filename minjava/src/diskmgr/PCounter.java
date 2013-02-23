package diskmgr;

public class PCounter {

	public static int counter;

	public static void initialize() {
		counter = 0;
	}

	public static void increment() {
		counter++;
	}

	public static int getCounter() {
		return counter;
	}

	public static void setCounter(int counter) {
		PCounter.counter = counter;
	}

}
