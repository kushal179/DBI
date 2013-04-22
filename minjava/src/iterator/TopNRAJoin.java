package iterator;

import global.AttrType;
import global.GlobalConst;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.xmlbeans.XmlCursor.TokenType;

import vo.Candidate;
import bufmgr.PageNotReadException;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;

public class TopNRAJoin extends Iterator implements GlobalConst {

	Collection<Candidate> topKCandidateList;
	int numTables = 0;
	String[] tableNameList;
	private static AttrType[][] inAttrType;
	private static int[] len_col;
	private static short[][] str_size;
	public AttrType[] jType;

	public TopNRAJoin(int _numTables, AttrType[][] in, int[] len_in,
			short[][] s_sizes, int[] join_col_in, Iterator[] am,
			int amt_of_mem, CondExpr[] outFilter, FldSpec[] proj_list,
			int n_out_flds, int num, String[] _tableNameList) {

		tableNameList = _tableNameList;
		numTables = _numTables;
		inAttrType = in;
		len_col = len_in;
		str_size = s_sizes;
		boolean whileFlag = true;
		int rowCount = 0;
		ListMultimap<String, Candidate> multimap = LinkedListMultimap.create();

		while (whileFlag) {
			ArrayList<Tuple> currList = new ArrayList<Tuple>();
			// After entire for loop we get single row by combining all table's
			// one row
			Tuple tt = null;
			for (int i = 0; i < numTables; i++) {

				if (join_col_in.length == numTables) {

					try {
						// TODO refine end criteria
						tt = ((FileScan) am[i]).get_next_NRA();
						if (tt != null) {
							currList.add(tt);
						} else {
							whileFlag = false;
							break;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else
					System.err
							.println("Count of Column Id does not match number of tables");
			}

			// System.out.println("row::" + currList);
			float threshold = 0;
			try {
				for (int i = 0; i < currList.size(); i++) {
					Candidate candidate = new Candidate();
					float lowScore = 0;
					float highScore = 0;
					String uniqueToken = tableNameList[i] + "#" + i + "#"+ rowCount;
					String patternMatch = tableNameList[i] + "#(.*)";
					Tuple currentTuple = currList.get(i);
					lowScore = (float) roundTwoDecimals(currentTuple.getScore());
					highScore = 0;
					String currentKey = currentTuple.getStrFld(join_col_in[i]);
					
					java.util.List<Candidate> mapList = multimap.get(currentKey);
					// for new entry
					if (mapList.size() == 0) {
						for (int j = i + 1; j < currList.size(); j++) {
							Tuple tempTuple = currList.get(j);
							String tempKey = tempTuple.getStrFld(join_col_in[j]);
							if (currentKey.equals(tempKey)) {
								lowScore = lowScore + (float) roundTwoDecimals(tempTuple.getScore());
							} else
								lowScore = (float) (lowScore + 0.0);

						}
						for (int k = 0; k < currList.size(); k++) {
							Tuple tempTuple = currList.get(k);
							highScore = highScore + (float) roundTwoDecimals(tempTuple.getScore());
						}
						candidate.setLowScore(lowScore);
						candidate.setHighScore(highScore);
						HashMap<String, Tuple> combinationMap = new HashMap<String, Tuple>();
						combinationMap.put(uniqueToken, currentTuple);
						candidate.setTopKMap(combinationMap);
						candidate.setKey(currentKey);
						multimap.put(currentKey, candidate);

					}
					// entry exists in multimap hence lookup for
					// <tableName_*,tuple> in candidate obj
					else {
						java.util.Iterator<Candidate> it = mapList.iterator();
						java.util.List<Candidate> updatedCandidateList = new ArrayList<Candidate>();
						while (it.hasNext()) {
							Candidate obj = it.next();
							HashMap<String, Tuple> mapObj = obj.getTopKMap();
							Set<String> keySet = mapObj.keySet();
							java.util.Iterator<String> keySetIt = keySet.iterator();
							boolean patterMatchFlag = false;
							while (keySetIt.hasNext()) {
								String key = keySetIt.next();
								if (key.matches(patternMatch)) {
									patterMatchFlag = true;
									break;
								}
							}
							if (patterMatchFlag) {
								updatedCandidateList.add(obj);
								Candidate incrementObj = new Candidate();
								HashMap<String, Tuple> tempMap = new HashMap<String, Tuple>();
								tempMap.put(uniqueToken, currentTuple);
								java.util.Iterator<String> tempIt = keySet.iterator();
								while (tempIt.hasNext()) {
									String tempKey = tempIt.next();
									if (!tempKey.matches(patternMatch)) {
										tempMap.put(tempKey,mapObj.get(tempKey));
									}
								}
								incrementObj.setTopKMap(tempMap);
								incrementObj.setKey(currentKey);
								Candidate updateObj = updateTopKMap(incrementObj, tempMap, currList);
								updatedCandidateList.add(updateObj);
							} else {
								mapObj.put(uniqueToken, currentTuple);
								obj.setTopKMap(mapObj);
								Candidate updateObj = updateTopKMap(obj,mapObj, currList);
								updatedCandidateList.add(updateObj);

							}

						}
						multimap.removeAll(currentKey);
						multimap.putAll(currentKey, updatedCandidateList);
					}
					threshold = threshold + (float) roundTwoDecimals(currentTuple.getScore());
					currentTuple = null;
					multimap = updateHSMultMap(multimap,currList);
					// here again iterate multimap and perform sort operation

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			rowCount++;

			multimap = sortMap(multimap);
			whileFlag = stoppingCondition(multimap, num, threshold);
			

		}
		//printMap(multimap);
		if (topKCandidateList != null) {
			System.out.println(topKCandidateList);

			try {
				if (get_next() == null)
					System.err.println("Key is not found in all the tables or TOPK result is empty");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public ListMultimap<String, Candidate> updateHSMultMap(ListMultimap<String, Candidate> map,ArrayList<Tuple> list)
	{
		ListMultimap<String, Candidate> updatedMap = LinkedListMultimap.create();
		Collection<Candidate> allCandidateObj = map.values();
		java.util.Iterator<Candidate> it = allCandidateObj.iterator();
		while(it.hasNext())
		{
			Candidate obj= it.next();
			HashMap<String,Tuple> hasMap = obj.getTopKMap();
			HashMap<Integer, Boolean> missingElementTrackMap = new HashMap<Integer, Boolean>();
			float highScore = obj.getLowScore();
			for (int i = 0; i < list.size(); i++) {
				missingElementTrackMap.put(i, false);
			}
			java.util.Iterator<String> ithasMap = hasMap.keySet().iterator();
			while(ithasMap.hasNext())
			{
			String key = ithasMap.next();
			String[] index = key.split("#");
			missingElementTrackMap.put(Integer.parseInt(index[1]), true);
			}
			for (int j = 0; j < missingElementTrackMap.size(); j++) {
				if (!missingElementTrackMap.get(j)) {
					try {
						float missingScore = (float) roundTwoDecimals(((Tuple) list.get(j)).getScore());
						highScore = highScore + missingScore;
					} catch (FieldNumberOutOfBoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			obj.setHighScore(highScore);
			updatedMap.put(obj.getKey(), obj);
		}
		
		return updatedMap;
	}
	public double roundTwoDecimals(double d) {
		DecimalFormat twoDForm = new DecimalFormat("#.####");
		return Double.valueOf(twoDForm.format(d));
	}

	public boolean stoppingCondition(ListMultimap<String, Candidate> multimap,
			int num, float threshold) {
		System.out.println("############################################");
		printMap(multimap);
		Collection<Candidate> sortedCandidate = multimap.values();
		topKCandidateList = new LinkedList<Candidate>();
		LinkedList<Candidate> llCandidate = new LinkedList<Candidate>(sortedCandidate);
		float tempLS=999;
		
		if (llCandidate.size() >= num) {
			for(int j=0;j<num;j++)
			{
				tempLS = Math.min(tempLS, llCandidate.get(j).getLowScore());
			}
				if (threshold < tempLS) {
					for (int k = 0; k < num; k++) {
						Candidate c = llCandidate.get(k);
						if(c.getTopKMap().size() < numTables)
							return true;
						topKCandidateList.add(c);
					}
					return false;
				} else
					return true;
		} else {
			for(int j=0;j<llCandidate.size();j++)
			{
				tempLS = Math.min(tempLS, llCandidate.get(j).getLowScore());
			}
			if (threshold < tempLS) {
				for (int k = 0; k < num; k++) {
					Candidate c = llCandidate.get(k);
					if(c.getTopKMap().size() < numTables)
						return true;
					topKCandidateList.add(c);
				}
				return false;
			} else
				return true;
		}
	}

	public ListMultimap<String, Candidate> sortMap(ListMultimap<String, Candidate> multimap) {
		Collection<Candidate> sortedCandidate = multimap.values();
		LinkedList<Candidate> llCandidate = new LinkedList<Candidate>(
				sortedCandidate);
		Collections.sort(llCandidate, new Comparator<Candidate>() {
			public int compare(Candidate o1, Candidate o2) {
				//float ls1 = o1.getLowScore();
				//float ls2 = o2.getLowScore();
				
				float ls1 = o1.getHighScore();
				float ls2 = o2.getHighScore();

				if (ls1 == ls2)
					return 0;
				else if (ls1 > ls2)
					return -1;
				else
					return 1;
			}
		});
		multimap.clear();
		java.util.Iterator<Candidate> sortedIt = llCandidate.iterator();
		while (sortedIt.hasNext()) {
			Candidate sortedObj = sortedIt.next();
			multimap.put(sortedObj.getKey(), sortedObj);
		}

		return multimap;
	}

	public void printMap(Multimap<String, Candidate> multimap) {
		Collection<Candidate> sortedCandidate = multimap.values();
		java.util.Iterator<Candidate> cit = sortedCandidate.iterator();
		System.out.println("--------------------------------");
		while (cit.hasNext()) {
			Candidate obj = cit.next();
			System.out.println("Key:" + obj.getKey() + "--LS:"+ obj.getLowScore() + "--HS:" + obj.getHighScore()+"--Map:"+obj.getTopKMap());
		}
	}

	/*
	 * public ListMultimap<String,Candidate>
	 * multiMapSort(ListMultimap<String,Candidate> map){
	 * 
	 * TreeMultimap<String, Candidate> treeMap =
	 * TreeMultimap.create(Ordering.arbitrary(), new Comparator<Candidate>() {
	 * public int compare(Candidate o1,Candidate o2){ float ls1 =
	 * o1.getLowScore(); float ls2 = o2.getLowScore();
	 * 
	 * if(ls1 == ls2) return 0; else if(ls1 > ls2) return -1; else return 1;
	 * }}); treeMap.putAll(map); Collection<Candidate> sortedCandidate =
	 * treeMap.values(); java.util.Iterator<Candidate> cit =
	 * sortedCandidate.iterator();
	 * System.out.println("**************************"); while(cit.hasNext()) {
	 * Candidate obj = cit.next();
	 * System.out.println("Key:"+obj.getKey()+"--LS:"
	 * +obj.getLowScore()+"--HS:"+obj.getHighScore()); } ListMultimap<String,
	 * Candidate> sortedMultiMap = LinkedListMultimap.create(treeMap);
	 * 
	 * 
	 * return sortedMultiMap; }
	 */
	public Candidate updateTopKMap(Candidate obj, HashMap<String, Tuple> map,
			ArrayList currList) {
		HashMap<Integer, Boolean> missingElementTrackMap = new HashMap<Integer, Boolean>();
		for (int i = 0; i < currList.size(); i++) {
			missingElementTrackMap.put(i, false);
		}
		java.util.Iterator<Entry<String, Tuple>> it = map.entrySet().iterator();
		float lowScore = 0, highScore = 0;

		if (it != null) {
			while (it.hasNext()) {
				Entry<String, Tuple> entry = it.next();
				Tuple t = entry.getValue();
				String key = entry.getKey();
				String[] index = key.split("#");
				missingElementTrackMap.put(Integer.parseInt(index[1]), true);
				try {
					lowScore = lowScore + (float) roundTwoDecimals(t.getScore());
					highScore = highScore + (float) roundTwoDecimals(t.getScore());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (map.size() != currList.size()) {
				/*
				 * java.util.Iterator<Entry<Integer,Boolean>>
				 * missingElementTrackMapIt =
				 * missingElementTrackMap.entrySet().iterator();
				 * while(missingElementTrackMapIt.hasNext()){
				 * Entry<Integer,Boolean> missingElementTrackMapEntry =
				 * missingElementTrackMapIt.next(); missingElementTrackMapEntry.
				 */
				for (int j = 0; j < missingElementTrackMap.size(); j++) {
					if (!missingElementTrackMap.get(j)) {
						try {
							float missingScore = (float) roundTwoDecimals(((Tuple) currList.get(j)).getScore());
							highScore = highScore + missingScore;
						} catch (FieldNumberOutOfBoundException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
			obj.setHighScore(highScore);
			obj.setLowScore(lowScore);
		} else {
			System.err.println("Map for updation cannot be empty");
		}
		return obj;

	}

	public String keyLookupFromMap(String pattern, HashMap<String, Tuple> map) {
		String key = null;
		Set<String> keySet = map.keySet();
		String[] keyArray = Arrays.copyOf(keySet.toArray(),
				keySet.toArray().length, String[].class);
		for (int k = 0; k < keyArray.length; k++) {
			key = keyArray[k];
			if (key.matches(pattern))
				return key;
		}
		return key;
	}

	// This will print all the topK tuples generated by NRA on a single call
	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, TupleUtilsException, PredEvalException,
			SortException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, Exception {

		if (topKCandidateList != null && topKCandidateList.size() != 0) {
			java.util.Iterator<Candidate> topKIterator = topKCandidateList
					.iterator();
			while (topKIterator.hasNext()) {
				Candidate obj = topKIterator.next();
				HashMap<String, Tuple> printMap = obj.getTopKMap();
				Tuple[] tupleArray = new Tuple[numTables];
				if (!(printMap.size() < numTables)) {
					for (int i = 0; i < numTables; i++) {
						String patternForKeyLookUp = tableNameList[i] + "#" + i
								+ "(.*)";
						String key = keyLookupFromMap(patternForKeyLookUp,
								printMap);
						if (key != null) {
							tupleArray[i] = printMap.get(key);
						} else
							return null;
					}
				} else
					return null;
				// one by one create structures in order to print the tuple
				Tuple t1 = tupleArray[0];
				Tuple t2 = tupleArray[1];
				Tuple Jtuple = new Tuple();

				// create proj_list
				int proj_len = len_col[0] + len_col[1] - 2;
				FldSpec[] proj = new FldSpec[proj_len];
				for (int i = 0; i < len_col[0] - 1; i++) {
					proj[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
				}
				for (int i = 0; i < len_col[1] - 1; i++) {
					proj[i + len_col[0] - 1] = new FldSpec(new RelSpec(
							RelSpec.innerRel), i + 1);
				}

				// create jtype
				jType = new AttrType[proj_len + 1];
				short[] t_size = TupleUtils.setup_op_tuple(Jtuple, jType,
						inAttrType[0], len_col[0], inAttrType[1], len_col[1],
						str_size[0], str_size[1], proj, proj_len, true);

				Projection.Join(t1, inAttrType[0], t2, inAttrType[1], Jtuple,
						proj, proj_len, true);

				for (int i = 2; i < numTables; i++) {
					short[] laststr_size = t_size;
					AttrType[] lastType = jType;
					int lastres = jType.length;
					Tuple temp3 = tupleArray[i];
					proj_len = proj_len + len_col[i] - 1;
					Tuple lasttuple = new Tuple();// =Jtuple;
					lasttuple.setHdr((short) lastres, lastType, laststr_size);
					lasttuple.tupleCopy(Jtuple);
					// create proj;
					proj = new FldSpec[proj_len];
					for (int j = 0; j < lastres - 1; j++) {
						proj[j] = new FldSpec(new RelSpec(RelSpec.outer), j + 1);
					}
					for (int j = 0; j < len_col[i] - 1; j++) {
						proj[j + lastres - 1] = new FldSpec(new RelSpec(
								RelSpec.innerRel), j + 1);
					}
					jType = new AttrType[proj_len + 1];

					t_size = TupleUtils.setup_op_tuple(Jtuple, jType, lastType,
							lastres, inAttrType[i], len_col[i], laststr_size,
							str_size[i], proj, proj_len, true);
					Projection.Join(lasttuple, lastType, temp3, inAttrType[i],
							Jtuple, proj, proj_len, true);
				}
				Jtuple.print(jType);
			}
		} else
			return null;

		return null;
	}

	@Override
	public void close() throws IOException, JoinsException, SortException,
			IndexException {

	}

}
