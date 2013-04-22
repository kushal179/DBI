package vo;

import global.RID;
import heap.Tuple;

import java.util.ArrayList;
import java.util.HashMap;

public class Candidate {
	
	private float lowScore;
	private float highScore;
	private ArrayList<RID> rID;
	private HashMap<String,Tuple> topKMap;
	private String key;
	public Candidate() {
		this.lowScore = 0;
		this.highScore = 0;
		rID = null;
		topKMap = null;
		this.key = "";
	}
	
	public Candidate(float ls,float hs)
	{
		this.lowScore = ls;
		this.highScore = hs;
		rID = null;
		topKMap = null;
		this.key = "";
	}
	
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public HashMap<String, Tuple> getTopKMap() {
		return topKMap;
	}
	public void setTopKMap(HashMap<String, Tuple> topKMap) {
		this.topKMap = topKMap;
	}
	public float getLowScore() {
		return lowScore;
	}
	public void setLowScore(float lowScore) {
		this.lowScore = lowScore;
	}
	public float getHighScore() {
		return highScore;
	}
	public void setHighScore(float highScore) {
		this.highScore = highScore;
	}
	public ArrayList<RID> getrID() {
		return rID;
	}
	public void setrID(ArrayList<RID> rID) {
		this.rID = rID;
	}
	
	

}
