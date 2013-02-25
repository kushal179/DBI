package main;

import heap.Heapfile;

public class MetaData {
	
	private String heapFileName;
	private int numOfAttr;
	private int numofStrAttr;
	private int numofNonStrAttr;
	private Heapfile heapFileObj;
	
	public String getHeapFileName() {
		return heapFileName;
	}
	public void setHeapFileName(String heapFileName) {
		this.heapFileName = heapFileName;
	}
	public int getNumOfAttr() {
		return numOfAttr;
	}
	public void setNumOfAttr(int numOfAttr) {
		this.numOfAttr = numOfAttr;
	}
	public int getNumofStrAttr() {
		return numofStrAttr;
	}
	public void setNumofStrAttr(int numofStrAttr) {
		this.numofStrAttr = numofStrAttr;
	}
	public int getNumofNonStrAttr() {
		return numofNonStrAttr;
	}
	public void setNumofNonStrAttr(int numofNonStrAttr) {
		this.numofNonStrAttr = numofNonStrAttr;
	}
	public Heapfile getHeapFileObj() {
		return heapFileObj;
	}
	public void setHeapFileObj(Heapfile heapFileObj) {
		this.heapFileObj = heapFileObj;
	}
	
	
	

}
