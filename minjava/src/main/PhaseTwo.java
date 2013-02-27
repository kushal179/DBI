package main;

import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
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
import iterator.DuplElim;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;

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
					//System.out.print(cell.getNumericCellValue() + "\t\t");
					numofNonStrAttr++;
					if (index == numAttrField)
						t.setScore((float) cell.getNumericCellValue());
					else
						t.setStrFld(index, cell.getStringCellValue());
					break;
				case Cell.CELL_TYPE_STRING:
					//System.out.print(cell.getStringCellValue() + "\t\t");
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
			System.out.println("PAGE ACCESS::"+PCounter.counter);
		}
		tableMap.put(fileName, metaData);
		file.close();
		

	}

	private String getHeapFileName(String fileName) {
		String string[] = fileName.split("\\.");
		StringBuilder builder = new StringBuilder();
		builder.append(string[0]).append(".in");
		return builder.toString();

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
			System.out.println("PAGE ACCESS::"+PCounter.counter);
			

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
				distinct = new DuplElim(attTypes,(short)numOfAttr,Ssizes,fileScan,10,false);
				Tuple t = null;
				while ((t = distinct.get_next()) != null) {
					t.print(attTypes);
				}
			} catch (Exception e) {
				System.err.println("" + e);
			}

		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int ch = 0;
		PhaseTwo obj = new PhaseTwo();
		while (ch < 7) {
			System.out
					.println("************* PLEASE SELECT ONE OF THE FOLLOWING OPTIONS ***************");
			System.out
					.println(" 1- Create a table \n 2- Print a table \n 3- Select Distinct query"
							+ "\n 4- Add tuples \n 5- Delete a tuple \n 6- Find top K SortMergeJoin");
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
				BufferedReader ip = new BufferedReader(
						new InputStreamReader(System.in));
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
				System.out.println("Find top K SortMergeJoin");
				break;

			default:
				System.out.println("Option not found");
				break;
			}

		}
	}

}
