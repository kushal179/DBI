package iterator;

import global.AttrType;
import global.GlobalConst;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import main.MetaData;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import vo.Candidate;
import bufmgr.PageNotReadException;

import com.google.common.collect.ListMultimap;

public class IncrementalNRA extends iterator.Iterator implements GlobalConst{
	
	
	
public IncrementalNRA(int _numTables, AttrType[][] in, int[] len_in,
		short[][] s_sizes, int[] join_col_in, Iterator[] am,
		int amt_of_mem, CondExpr[] outFilter, FldSpec[] proj_list,
		int n_out_flds, int num, String[] _tableNameList,String tableToBeUpdated, 
		String newFile, TopNRAJoin topNRAJoin){
	try {
		ListMultimap<String, Candidate> multimap = topNRAJoin.multimap;
		iterator.Iterator []  _am = topNRAJoin._am1;
		float threshold = 0;
		boolean whileFlag = true;
		MetaData data = new MetaData();
		
		ArrayList<Tuple> currList = new ArrayList<Tuple>();
		Tuple tt = null;
		for (int i = 0; i < _numTables; i++) {

			if (join_col_in.length == _numTables) {

				try {
					// TODO refine end criteria
					tt = ((FileScan) am[i]).get_next_NRA();
					if (tt != null) {
						currList.add(tt);
					} 

				} catch (Exception e) {
					e.printStackTrace();
				}
		
			}
		}
		
		FileInputStream file = new FileInputStream(new File(newFile));
		
		Workbook wb = WorkbookFactory.create(file);
		Sheet sheet = wb.getSheetAt(0);
		java.util.Iterator<Row> rowIterator = sheet.iterator();
		int numAttrField = 0;
		int numFields = 0;
		int numofStrAttr = 0;
		int i =0;
		int numofNonStrAttr = 0;
		int strNonStrCount = 0;
		Tuple t = new Tuple();
		while (rowIterator.hasNext()) {
			if (i == 0) {
				Row row = rowIterator.next();
				numAttrField = row.getLastCellNum();
				
				// Number of cells/attribute in xlxs sheet
				numFields = row.getLastCellNum();
				data.setNumOfAttr(numFields);
				AttrType[] attTypes = new AttrType[numFields];
				short[] Ssizes = new short[numFields - 1];
				for (int j = 0; j < numFields - 1; j++) {

					attTypes[j] = new AttrType(AttrType.attrString);
					Ssizes[j] = 30;
					data.setSsizes(Ssizes);
				}
				attTypes[numFields - 1] = new AttrType(AttrType.attrReal);
				i = 1;
				try {
					t.setHdr((short) numFields, attTypes, Ssizes);
				} catch (InvalidTypeException e1) {
					e1.printStackTrace();
				} catch (InvalidTupleSizeException e1) {
					e1.printStackTrace();
				}
				int size = t.size();
				t = new Tuple(size);
				try {
					t.setHdr((short) numFields, attTypes, Ssizes);
				} catch (Exception e) {
					System.err.println("*** error in Tuple.setHdr() ***");
					e.printStackTrace();
				}
			}
			// For each row, iterate through each columns
			else{
				Row row = rowIterator.next();
				// For each row, iterate through each columns
				java.util.Iterator<Cell> cellIterator = row.cellIterator();
				while (cellIterator.hasNext()) {

					Cell cell = cellIterator.next();
					int index = cell.getColumnIndex() + 1;
					switch (cell.getCellType()) {
					case Cell.CELL_TYPE_NUMERIC:
						// System.out.print(cell.getNumericCellValue() + "\t\t");
						numofNonStrAttr++;
						if (index == numAttrField)
							t.setScore((float) cell.getNumericCellValue());
						else
							t.setStrFld(index, ((Double) cell.getNumericCellValue()).toString());
						break;
					case Cell.CELL_TYPE_STRING:
						// System.out.print(cell.getStringCellValue() + "\t\t");
						numofStrAttr++;
						t.setStrFld(index, cell.getStringCellValue());
						break;
					}
				}
			
			
			String col = t.getStrFld(join_col_in[0]);
			if(multimap.containsKey(col)){
				List<Candidate> candidates = multimap.get(col);
				for(Candidate candidate : candidates){
					java.util.List<Candidate> updatedCandidateList = new ArrayList<Candidate>();
					HashMap<String, Tuple> topKMap = candidate.getTopKMap();
					String patternMatch = newFile + "#(.*)";
					String value = "";
						Set<String> keySet = topKMap.keySet();
						java.util.Iterator<String> keySetIt = keySet.iterator();
						boolean patterMatchFlag = false;
						String uniqueToken = "";
						
						while (keySetIt.hasNext()) {
							String key = keySetIt.next();
							if (key.matches(patternMatch)) {
								value = key.split("#")[1];
								patterMatchFlag = true;
								break;
							}
						}
						if (patterMatchFlag) {
							Candidate incrementObj = new Candidate();
							
							HashMap<String, Tuple> tempMap = new HashMap<String, Tuple>();
							uniqueToken = newFile + "#" + value + "#"+ "20000";
							tempMap.put(uniqueToken, t);
							java.util.Iterator<String> tempIt = keySet
									.iterator();
							while (tempIt.hasNext()) {
								String tempKey = tempIt.next();
								if (!tempKey.matches(patternMatch)) {
									tempMap.put(tempKey,topKMap.get(tempKey));
								}
							}
							incrementObj.setTopKMap(tempMap);
							incrementObj.setKey(col);
							Candidate updateObj = topNRAJoin.updateTopKMap(
									incrementObj, tempMap, currList);
							updatedCandidateList.add(updateObj);
						} else {
							topKMap.put(uniqueToken, t);
							candidate.setTopKMap(topKMap);
							Candidate updateObj = topNRAJoin.updateTopKMap(candidate,
									topKMap, currList);
							updatedCandidateList.add(updateObj);

						}
						multimap.removeAll(col);
						multimap.putAll(col, updatedCandidateList);
					}				
				threshold = threshold + (float) topNRAJoin.roundTwoDecimals(t.getScore());
				t = null;
				multimap = topNRAJoin.updateHSMultMap(multimap,currList);
				
					
						
						
					}

		
		multimap = topNRAJoin.sortMap(multimap);
		whileFlag = !(topNRAJoin.stoppingCondition(multimap, num, threshold));
		
		
		if(whileFlag){
			
			repeatNRA(multimap,topNRAJoin.tableNameList, );
			
			
			
			
			
			
		}
			
		}

	}
}


catch (FileNotFoundException e) {
		e.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	} catch (InvalidFormatException e) {
		e.printStackTrace();
	} catch (Exception e) {
		e.printStackTrace();
	}
}
	@Override
	public void close() throws IOException, JoinsException, SortException,
			IndexException {

	}

	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException,
			TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		return null;
	}
	
	public ListMultimap<String, Candidate> repeatNRA(ListMultimap<String, Candidate> multimap){
	
		
		while (whileFlag) {
			ArrayList<Tuple> currList = new ArrayList<Tuple>();
			// After entire for loop we get single row by combining all table's
			// one row
			Tuple tt = null;
			for (int i = 0; i < numTables; i++) {

				if (join_col_in.length == numTables) {

					try {
						// TODO refine end criteria
						tt = ((FileScan) _am1[i]).get_next_NRA();
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
		
return multimap;	
		
		
	}
	

}
