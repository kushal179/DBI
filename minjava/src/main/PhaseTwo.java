package main;

import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import global.TupleOrder;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import iterator.CondExpr;
import iterator.DuplElim;
import iterator.FileIndex;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.TopNestedLoopsJoins;
import iterator.TopRankBuildIndex;
import iterator.TopRankJoin;
import iterator.TopSortMerge;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import bufmgr.BufMgrException;
import bufmgr.HashOperationException;
import bufmgr.PageNotFoundException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;

import diskmgr.PCounter;

public class PhaseTwo {

	int numAttrField = 0;
	MetaData metaData;
	static HashMap<String, MetaData> tableMap = new HashMap<String, MetaData>();

	public PhaseTwo() {
		String dbpath = "/tmp/" + System.getProperty("user.name")
				+ ".minibase.jointestdb";
		String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";

		String remove_cmd = "/bin/rm -rf ";
		String remove_logcmd = remove_cmd + logpath;
		String remove_dbcmd = remove_cmd + dbpath;
		String remove_joincmd = remove_cmd + dbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
			Runtime.getRuntime().exec(remove_joincmd);
		} catch (IOException e) {
			System.err.println("" + e);
		}

		SystemDefs sysdef = new SystemDefs(dbpath, 1000, GlobalConst.NUMBUF,
				"Clock");
	}

	/**
	 * @param fileName
	 *            File name of the table in xls format
	 */
	public void createTable(String fileName) throws IOException,
			FileNotFoundException, InvalidFormatException,
			FieldNumberOutOfBoundException, HFDiskMgrException,
			HFBufMgrException, HFException, SpaceNotAvailableException,
			InvalidTupleSizeException, InvalidSlotNumberException {
		FileInputStream file = new FileInputStream(new File(fileName));

		Workbook wb = WorkbookFactory.create(file);
		Sheet sheet = wb.getSheetAt(0);

		// Iterate through each rows from first sheet
		Iterator<Row> rowIterator = sheet.iterator();
		int i = 0;
		metaData = new MetaData();

		// sets heapFileName corresponding to xls file name(eg: R1.xls as R1.in)
		String heapFileName = getHeapFileName(fileName);
		metaData.setHeapFileName(heapFileName);
		Heapfile heapfile = makeHeapFile(metaData.getHeapFileName());
		int numofStrAttr = 0;
		int numofNonStrAttr = 0;
		int strNonStrCount = 0;
		Tuple t = new Tuple();
		while (rowIterator.hasNext()) {

			// Scans first row of the table which are the labels of the table
			if (i == 0) {
				Row row = rowIterator.next();
				// Number of cells/attribute in xlxs sheet
				numAttrField = row.getLastCellNum();
				metaData.setNumOfAttr(numAttrField);
				AttrType[] attTypes = new AttrType[numAttrField];
				short[] Ssizes = new short[numAttrField - 1];
				for (int j = 0; j < numAttrField - 1; j++) {

					attTypes[j] = new AttrType(AttrType.attrString);
					Ssizes[j] = 30;
					metaData.setSsizes(Ssizes);
				}
				attTypes[numAttrField - 1] = new AttrType(AttrType.attrReal);
				i = 1;
				try {
					t.setHdr((short) numAttrField, attTypes, Ssizes);
				} catch (InvalidTypeException e1) {
					e1.printStackTrace();
				} catch (InvalidTupleSizeException e1) {
					e1.printStackTrace();
				}
				int size = t.size();
				t = new Tuple(size);
				try {
					t.setHdr((short) numAttrField, attTypes, Ssizes);
				} catch (Exception e) {
					System.err.println("*** error in Tuple.setHdr() ***");
					e.printStackTrace();
				}
			}
			Row row = rowIterator.next();
			// For each row, iterate through each columns
			Iterator<Cell> cellIterator = row.cellIterator();
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
						t.setStrFld(index,
								((Double) cell.getNumericCellValue())
										.toString());
					break;
				case Cell.CELL_TYPE_STRING:
					// System.out.print(cell.getStringCellValue() + "\t\t");
					numofStrAttr++;
					t.setStrFld(index, cell.getStringCellValue());
					break;
				}
			}
			if (strNonStrCount == 0) {
				metaData.setNumofStrAttr(numofStrAttr);
				metaData.setNumofNonStrAttr(numofNonStrAttr);
				strNonStrCount = 1;
			}
			System.out.println("");
			RID rid = heapfile.insertRecord(t.returnTupleByteArray());
			System.out.println("RECORD ID SLOT NO:" + rid.slotNo);
			System.out.println("PAGE NO:" + rid.pageNo);
			System.out.println("BufCounter:" + PCounter.bufCounter);
			System.out.println("HfCounter:" + PCounter.hfCounter);
		}
		tableMap.put(fileName, metaData);
		file.close();

	}

	
	public void topKOperation(int k, String tableName1, String tableName2,
			int amtMemory, int joinCol1, int joinCol2, int operation) {
		// TODO: if time permits accept order from User i.e. ASC OR DESC
		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		TopSortMerge sm = null;
		TopNestedLoopsJoins nj = null;
		MetaData meta1 = tableMap.get(tableName1);
		MetaData meta2 = tableMap.get(tableName2);
		if (meta1 == null && meta2 == null) {
			System.err.println("No Matching table found");
		} else {

			try {
				CondExpr[] outFilter = new CondExpr[2];
				outFilter[0] = new CondExpr();
				outFilter[1] = new CondExpr();
				condExpr(outFilter, joinCol1, joinCol2);
				int totalNumAttr1 = meta1.getNumOfAttr();
				int totalNumAttr2 = meta2.getNumOfAttr();
				FldSpec[] proj_list = getFieldProjection(totalNumAttr1,
						totalNumAttr2);
				int innerCount = getCount(proj_list, RelSpec.innerRel);
				int outerCount = proj_list.length - innerCount;
				iterator.Iterator fileScan1 = null;
				iterator.Iterator fileScan2 = null;
				fileScan1 = new FileScan(getHeapFileName(tableName1),
						generateAttrTypeArray(totalNumAttr1),
						meta1.getSsizes(), (short) totalNumAttr1,
						(short) totalNumAttr1, projections(totalNumAttr1), null);
				fileScan2 = new FileScan(getHeapFileName(tableName2),
						generateAttrTypeArray(totalNumAttr2),
						meta2.getSsizes(), (short) totalNumAttr2,
						(short) totalNumAttr2, projections(totalNumAttr2), null);
				switch(operation){
				case GlobalConst.TOPKSORTMERGER:
				sm = new TopSortMerge(generateAttrTypeArray(totalNumAttr1),
						totalNumAttr1, meta1.getSsizes(),
						generateAttrTypeArray(totalNumAttr2), totalNumAttr2,
						meta2.getSsizes(), joinCol1, 30, joinCol2, 30,
						amtMemory, fileScan1, fileScan2, false, false,
						ascending, outFilter, proj_list, totalNumAttr1
								+ totalNumAttr2, k, innerCount, outerCount);
				System.out.println("HFCounter After TOPKSORTMERGEJOIN: "+ PCounter.hfCounter);
				System.out.println("BufCounter After TOPKSORTMERGEJOIN: "+ PCounter.bufCounter);
			//	SystemDefs.JavabaseBM.flushAllPages();
				
				break;
				case GlobalConst.TOPKNESTEDJOIN:
					//relationName  access heapfile for right i/p to join
					String relationName = getHeapFileName(tableName2);
					nj = new TopNestedLoopsJoins(generateAttrTypeArray(totalNumAttr1), totalNumAttr1, 
							meta1.getSsizes(), generateAttrTypeArray(totalNumAttr2), 
							totalNumAttr2, meta2.getSsizes(), 
							amtMemory, fileScan1, 
							relationName, outFilter, 
							null, proj_list, 
							totalNumAttr1+ totalNumAttr2, k,innerCount,outerCount);
					System.out.println("HFCounter After TOPKNESTEDJOIN: "+ PCounter.hfCounter);
					System.out.println("BufCounter After TOPKNESTEDJOIN: "+ PCounter.bufCounter);
					//SystemDefs.JavabaseBM.flushAllPages();
					break;
					
				default:
					System.out.println("No Matching operation found");
				
				}

				
			} catch (Exception e) {
				System.err.println("" + e);
				e.printStackTrace();
			}
		}
	}

	private void condExpr(CondExpr[] expr, int joinCol1, int joinCol2) {
		expr[0].next = null;
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
				joinCol1);// Relation_1 outer
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),
				joinCol2);// Relation_1 inner
		expr[1] = null;

	}

	// this method returns the count of inner or outer relation based on type
	// field
	private int getCount(FldSpec[] projList, int type) {
		int incount = 0;
		for (int i = 0; i < projList.length; i++) {
			if (projList[i].relation.key == type)
				incount++;
		}
		return incount;

	}

	// this method returns the fieldProjection
	private FldSpec[] getFieldProjection(int totalNumAttr1, int totalNumAttr2) {
		FldSpec[] proj_list = new FldSpec[totalNumAttr1 + totalNumAttr2];
		if (totalNumAttr1 == 0 && totalNumAttr2 == 0) {
			System.err.println("Please enter non zero attribute count");
			return null;
		} else {

			int offSetCount = 1;
			int i;
			for (i = 0; i < totalNumAttr1; i++) {
				proj_list[i] = new FldSpec(new RelSpec(RelSpec.innerRel),
						offSetCount);
				offSetCount++;
			}
			offSetCount = 1;
			for (int j = 0; j < totalNumAttr2; j++) {
				proj_list[i] = new FldSpec(new RelSpec(RelSpec.outer),
						offSetCount);
				offSetCount++;
				i++;
			}

		}

		return proj_list;

	}

	private String getHeapFileName(String fileName) {
		String string[] = fileName.split("\\.");
		StringBuilder builder = new StringBuilder();
		builder.append(string[0]).append(".in");
		return builder.toString();

	}

	public AttrType[] generateAttrTypeArray(int totalNumOfAttr) {
		AttrType[] attrType = new AttrType[totalNumOfAttr];

		for (int i = 0; i < totalNumOfAttr - 1; i++) {
			attrType[i] = new AttrType(AttrType.attrString);

		}
		attrType[totalNumOfAttr - 1] = new AttrType(AttrType.attrReal);

		return attrType;
	}

	private Heapfile makeHeapFile(String fileName) {

		Heapfile heapfile = null;
		try {
			heapfile = new Heapfile(fileName);

		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}
		return heapfile;
	}

	public FldSpec[] projections(int offSetCount) {
		FldSpec[] projections = new FldSpec[offSetCount];
		for (int i = 0; i < offSetCount; i++) {
			projections[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
		}
		return projections;
	}

	private void printTable(String fileName) {
		MetaData meta = tableMap.get(fileName);
		if (meta == null)
			System.err.println("No Matching table found");
		else {
			iterator.Iterator fileScan = null;
			int numOfAttr = meta.getNumOfAttr();
			AttrType[] attTypes = new AttrType[numOfAttr];
			short[] Ssizes = new short[numOfAttr - 1];
			for (int j = 0; j < numOfAttr - 1; j++) {

				attTypes[j] = new AttrType(AttrType.attrString);
				Ssizes[j] = 30;
			}
			attTypes[numOfAttr - 1] = new AttrType(AttrType.attrReal);
			try {
				fileScan = new FileScan(getHeapFileName(fileName), attTypes,
						Ssizes, (short) numOfAttr, (short) numOfAttr,
						projections(numOfAttr), null);
				Tuple t = null;
				while ((t = fileScan.get_next()) != null) {
					t.print(attTypes);
				}
			} catch (Exception e) {
				System.err.println("" + e);
			}
			System.out.println("HFCounter After TABLE CREATION: "+ PCounter.hfCounter);
			System.out.println("BufCounter After TABLE CREATION: "+ PCounter.bufCounter);
			
			/*try {
				SystemDefs.JavabaseBM.flushAllPages();
			} catch (HashOperationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (PageUnpinnedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (PagePinnedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (PageNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (BufMgrException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
*/
		}
	}

	private void printDistinctTuple(String fileName) {
		MetaData meta = tableMap.get(fileName);
		if (meta == null)
			System.err.println("No Matching table found");
		else {
			iterator.Iterator fileScan = null;
			DuplElim distinct = null;
			int numOfAttr = meta.getNumOfAttr();
			AttrType[] attTypes = new AttrType[numOfAttr];
			short[] Ssizes = new short[numOfAttr - 1];
			for (int j = 0; j < numOfAttr - 1; j++) {

				attTypes[j] = new AttrType(AttrType.attrString);
				Ssizes[j] = 30;
			}
			attTypes[numOfAttr - 1] = new AttrType(AttrType.attrReal);
			try {
				fileScan = new FileScan(getHeapFileName(fileName), attTypes,
						Ssizes, (short) numOfAttr, (short) numOfAttr,
						projections(numOfAttr), null);
				distinct = new DuplElim(attTypes, (short) numOfAttr, Ssizes,
						fileScan, 10, false);
				Tuple t = null;
				while ((t = distinct.get_next()) != null) {
					t.print(attTypes);
				}
			} catch (Exception e) {
				System.err.println("" + e);
			}

		}
		
	
	}
	private void topRankJoin(int numTables,String[] tableNameList,int[] joinColId,int amtMemory,int num){
		AttrType[][] in = new AttrType[numTables][];		//provides list of field types for each table
		short[][] s_sizes = new short[numTables][];			//provides the length of the string fields in each relation
		int[] len_in = new int[numTables];					//provides # of columns of each table
		String[] tableHeapName = new String[numTables];		//provides # of heapfile corresponding to each table created
		String[] indNames = new String[numTables];			//provides the index file name for each field that is being joined
		int n_out_flds = 0;
		for(int i=0;i<numTables;i++){
			MetaData meta = tableMap.get(tableNameList[i]);
			s_sizes[i] = meta.getSsizes();
			len_in[i] = meta.getNumOfAttr();
			in[i] = generateAttrTypeArray(meta.getNumOfAttr());
			tableHeapName[i] = meta.getHeapFileName();
			indNames[i] = "BtreeIndex"+(i+1);
			n_out_flds = n_out_flds + meta.getNumOfAttr();;
		}
		//FldSpec[] proj_list = projections(n_out_flds);
		TopRankBuildIndex topRankBuildIndex = new TopRankBuildIndex();
		System.out.println("**************get FileIndex***************");
		try{
		FileIndex[] BTfile = topRankBuildIndex.getFileIndex(s_sizes, in, len_in,
				numTables, tableHeapName, 1, indNames, joinColId, n_out_flds);

		System.out.println("**************get SortedIterator ***************");
		iterator.Iterator[] am = topRankBuildIndex.getSortedIterator(s_sizes, in, len_in,
				numTables, tableHeapName, 1,n_out_flds);
		
		TopRankJoin rj = new TopRankJoin(numTables, joinColId, am, num, BTfile, s_sizes,
				in, 1 , n_out_flds, len_in, amtMemory);
		
		
		for(int i=0;i<numTables;i++){
		System.out.println(" Num of Scaned Tuple for TABLE "+(i+1)+" is:" + rj.num_scanned(i));
		System.out.println(" Num of Probe Tuple for TABLE "+(i+1)+" is:" + rj.num_probed(i));
		}
		Tuple t=new Tuple();
		while((t=rj.get_next())!=null)
		{
			t.print(rj.jType);
		}
		rj.close();
		System.out.println("HFCounter After TOPKRANKJOIN: "+ PCounter.hfCounter);
		System.out.println("BufCounter After TOPKRANKJOIN: "+ PCounter.bufCounter);
	//	SystemDefs.JavabaseBM.flushAllPages();
		}
		
		catch(Exception e){
			e.printStackTrace();
		}
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int ch = 0;
		PhaseTwo obj = new PhaseTwo();
		while (ch < 8) {
			System.out
					.println("************* PLEASE SELECT ONE OF THE FOLLOWING OPTIONS ***************");
			System.out
					.println(" 1- Create a table \n 2- Print a table \n 3- Select Distinct query"
							+ "\n 4- Add tuples \n 5- Delete a tuple \n 6- Find top K SortMergeJoin"
							+ "\n 7 Find top K NestedJoin \n 8 Find top K Rank Join");
			System.out
					.println("************************************************************************");
			BufferedReader lineOfText = new BufferedReader(
					new InputStreamReader(System.in));
			try {
				String textLine = lineOfText.readLine();
				ch = Integer.parseInt(textLine);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (NumberFormatException e) {
				System.err.println("Trouble with the parsing of the number");
			}

			switch (ch) {
			case 1:

				System.out.println("Enter xls file name:");
				BufferedReader fileName = new BufferedReader(
						new InputStreamReader(System.in));
				try {
					String file = fileName.readLine();

					obj.createTable(file);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InvalidFormatException e) {
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
				System.out.println("Table created");

				break;

			case 2:
				System.out.println("SELECT * FROM <table Name>");
				System.out.println("Enter table name including .xlsx:");
				BufferedReader fName = new BufferedReader(
						new InputStreamReader(System.in));
				try {
					String file = fName.readLine();
					obj.printTable(file);
				} catch (IOException e) {
					e.printStackTrace();
				}

				break;
			case 3:
				System.out.println("SELECT DISTINCT * FROM <table Name>");
				System.out.println("Enter table name including .xlsx:");
				BufferedReader ip = new BufferedReader(new InputStreamReader(
						System.in));
				try {
					String file = ip.readLine();
					obj.printDistinctTuple(file);
				} catch (IOException e) {
					e.printStackTrace();
				}
				break;
			case 4:
				System.out.println("Add tuples");
				break;

			case 5:
				System.out.println("Delete a tuple");
				break;

			case 6:
				System.out
						.println("*************Find top K SortMergeJoin****************");

				try {
					System.out.println("Enter Top K value:");
					BufferedReader br = new BufferedReader(
							new InputStreamReader(System.in));
					int k = Integer.parseInt(br.readLine());
					System.out.println("Enter Name of Relation 1:");
					br = new BufferedReader(new InputStreamReader(System.in));
					String tableName1 = br.readLine();
					System.out.println("Enter Name of Relation 2:");
					br = new BufferedReader(new InputStreamReader(System.in));
					String tableName2 = br.readLine();

					System.out.println("Enter Amount of Memory:");
					br = new BufferedReader(new InputStreamReader(System.in));
					int amtMemory = Integer.parseInt(br.readLine());

					System.out
							.println("Enter JOIN offset (COL ID) for innerRel:");
					br = new BufferedReader(new InputStreamReader(System.in));
					int joinCol1 = Integer.parseInt(br.readLine());

					/*
					 * System.out.println("Enter SORT offset (COL ID) for innerRel:"
					 * ); br = new BufferedReader(new
					 * InputStreamReader(System.in)); int sortCol1 =
					 * Integer.parseInt(br.readLine());
					 */

					System.out
							.println("Enter JOIN offset (COL ID) for outerRel:");
					br = new BufferedReader(new InputStreamReader(System.in));
					int joinCol2 = Integer.parseInt(br.readLine());

					/*
					 * System.out.println("Enter SORT offset (COL ID) for outerRel:"
					 * ); br = new BufferedReader(new
					 * InputStreamReader(System.in)); int sortCol2 =
					 * Integer.parseInt(br.readLine());
					 */

					obj.topKOperation(k, tableName1, tableName2, amtMemory,
							joinCol1, joinCol2, GlobalConst.TOPKSORTMERGER);

				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

				break;
			case 7:
				System.out
						.println("*************Find top K Nested Join****************");

				try {
					System.out.println("Enter Top K value:");
					BufferedReader br = new BufferedReader(
							new InputStreamReader(System.in));
					int k = Integer.parseInt(br.readLine());
					System.out.println("Enter Name of Relation 1:");
					br = new BufferedReader(new InputStreamReader(System.in));
					String tableName1 = br.readLine();
					System.out.println("Enter Name of Relation 2:");
					br = new BufferedReader(new InputStreamReader(System.in));
					String tableName2 = br.readLine();

					System.out.println("Enter Amount of Memory:");
					br = new BufferedReader(new InputStreamReader(System.in));
					int amtMemory = Integer.parseInt(br.readLine());

					System.out
							.println("Enter JOIN offset (COL ID) for innerRel:");
					br = new BufferedReader(new InputStreamReader(System.in));
					int joinCol1 = Integer.parseInt(br.readLine());

					/*
					 * System.out.println("Enter SORT offset (COL ID) for innerRel:"
					 * ); br = new BufferedReader(new
					 * InputStreamReader(System.in)); int sortCol1 =
					 * Integer.parseInt(br.readLine());
					 */

					System.out
							.println("Enter JOIN offset (COL ID) for outerRel:");
					br = new BufferedReader(new InputStreamReader(System.in));
					int joinCol2 = Integer.parseInt(br.readLine());

					/*
					 * System.out.println("Enter SORT offset (COL ID) for outerRel:"
					 * ); br = new BufferedReader(new
					 * InputStreamReader(System.in)); int sortCol2 =
					 * Integer.parseInt(br.readLine());
					 */

					obj.topKOperation(k, tableName1, tableName2, amtMemory,
							joinCol1, joinCol2, GlobalConst.TOPKNESTEDJOIN);

				} catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				break;
			case 8:
				System.out
					.println("*************Find top K Rank Join****************");
				try {
					BufferedReader br;
					System.out.println("Enter Top K value:");
					br = new BufferedReader(
							new InputStreamReader(System.in));
					int k = Integer.parseInt(br.readLine());
					
					System.out.println("Enter Number of relation");
					br = new BufferedReader(
							new InputStreamReader(System.in));
					int numTables = Integer.parseInt(br.readLine());
					String[] tableNameList = new String[numTables];
					int[] joinColId = new int[numTables];
					for(int i=0;i<numTables;i++){
						System.out.println("Enter Name of Relation "+(i+1)+":");
						br = new BufferedReader(new InputStreamReader(System.in));
						tableNameList[i] = br.readLine();
						
						System.out.println("Enter offset(Join Col id) for Relation "+(i+1)+":");
						br = new BufferedReader(new InputStreamReader(System.in));
						joinColId[i] = Integer.parseInt(br.readLine());
					}
					System.out.println("Enter Amount of Memory:");
					br = new BufferedReader(new InputStreamReader(System.in));
					int amtMemory = Integer.parseInt(br.readLine());
					
					obj.topRankJoin(numTables, tableNameList, joinColId, amtMemory, k);
				}catch (NumberFormatException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				break;

			default:
				System.out.println("Option not found");
				break;
			}

		}
	}

}
