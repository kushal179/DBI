package diskmgr;

public class PCounter {

	public static int hfCounter; //counter for heap file
	public static int bufCounter; //counter for buffer manager

	//Methods for buffer manager
	public static int getBufCounter() {
		return bufCounter;
	}

	public static void setBufCounter(int bufCounter) {
		PCounter.bufCounter = bufCounter;
	}
	
	public static void bufInitialize() {
		bufCounter = 0;
	}

	public static void bufIncrement() {
		bufCounter++;
	}
	
	
	//Methods for heap file

	public static int getHfCounter() {
		return hfCounter;
	}

	public static void setHfCounter(int hfCounter) {
		PCounter.hfCounter = hfCounter;
	}

	public static void hfInitialize() {
		hfCounter = 0;
	}

	public static void hfIncrement() {
		hfCounter++;
	}

	
}
