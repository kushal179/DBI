package tests;

import global.AttrType;
import global.RID;
import global.SystemDefs;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import iterator.DuplElim;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import global.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class PhaseTwoTest {

	int numFields = 0;
	/**
	 * @param args
	 * @throws IOException
	 * @throws FieldNumberOutOfBoundException
	 * @throws InvalidTupleSizeException
	 * @throws InvalidTypeException
	 */
	public static void main(String[] args) throws IOException,
			FieldNumberOutOfBoundException, InvalidTypeException,
			InvalidTupleSizeException {
		// TODO Auto-generated method stub
		PhaseTwoTest pt = new PhaseTwoTest();

		pt.readFile("r1.txt");
		// pt.readFile("r2.txt");

	}

	public PhaseTwoTest() {
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

	public void readFile(String fileName) throws IOException,
			FieldNumberOutOfBoundException, InvalidTypeException,
			InvalidTupleSizeException {
		BufferedReader readbuffer = new BufferedReader(new FileReader(fileName));
		String strRead;
		RID rid;
		String newFileName;
		Tuple t = new Tuple();
		Heapfile heapfile = null;
		newFileName = getFileName(fileName);
		heapfile = makeHeapFile(newFileName);
		int i = 0;
		
		while ((strRead = readbuffer.readLine()) != null) {
			String splitString[] = strRead.split("\t");
			numFields = splitString.length;
			if (i == 0) {
				AttrType[] attTypes = new AttrType[numFields];
				short[] Ssizes = new short[numFields - 1];
				for (int j = 0; j < numFields - 1; j++) {

					attTypes[j] = new AttrType(AttrType.attrString);
					Ssizes[j] = 30;
				}
				attTypes[numFields - 1] = new AttrType(AttrType.attrReal);
				i = 1;
				t.setHdr((short) numFields, attTypes, Ssizes);
				int size = t.size();
				t = new Tuple(size);
				try {
					t.setHdr((short) numFields, attTypes, Ssizes);
				} catch (Exception e) {
					System.err.println("*** error in Tuple.setHdr() ***");
					e.printStackTrace();
				}
				// change the code
			}

			for (int j = 0; j < numFields - 1; j++) {
				t.setStrFld(j + 1, splitString[j]);
			}
			t.setScore(Float.parseFloat(splitString[numFields - 1]));
			//System.out.println(strRead);

			try {
				rid = heapfile.insertRecord(t.returnTupleByteArray());
			} catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}

		}
		printRecord(newFileName, generateAttrTypeArray(numFields-1, 0, 1), generateLengthOfStringFields(numFields-1, (short)30), 3, 3, 3);
		
		
		

	}
	//created: kushal
	public void printRecord(String fileName, AttrType[] attTypes,
			short[] Ssizes, int numInFields,int numOutFields, int offSetCount) {
		
		iterator.Iterator am = null;
		DuplElim ed = null;
		try {
			am = new FileScan(fileName, attTypes, Ssizes, (short) numInFields,
					(short) numOutFields, projections(offSetCount), null);
			
			//dulicate elimination
			
		      ed = new DuplElim(generateAttrTypeArray(numFields-1, 0, 1), (short)3, generateLengthOfStringFields(numFields-1, (short)30), am, 10, false);
			
			
		} catch (Exception e) {
			System.err.println("" + e);
		}
		Tuple t = null;
		/*try {
			while ((t = am.get_next()) != null) {
				t.print(attTypes);
				System.out.println(t.getScore());
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}*/
		//System.out.println("*************ELIMINATE DUPLICATE RECORDS****************");
		//t = null;
		try {
			System.out.println("*************ELIMINATE DUPLICATE RECORDS****************");
			while ((t = ed.get_next()) != null) {
				t.print(attTypes);
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

	}
	
	//created: kushal
	public short[] generateLengthOfStringFields(int numOfStrFields,short sizeOfStr){
		short[] strSizes = new short[numOfStrFields];
		for(int i=0;i<numOfStrFields;i++){
			strSizes[i] = sizeOfStr;
		}
		return strSizes;
	}
	//created: kushal
	//pass count for each attribute type. If not needed pass 0
	public AttrType[] generateAttrTypeArray(int numOfStr,int numOfInt,int numOfReal){
		int totalNumOfAttr = numOfStr + numOfInt + numOfReal;
		AttrType[] attrType = new AttrType[totalNumOfAttr];
		int count = 0;
		
		for(int i=0;i<numOfStr;i++){
			attrType[count] = new AttrType(AttrType.attrString);
			count++;
		}
		for(int i=0;i<numOfInt;i++){
			attrType[count] = new AttrType(AttrType.attrInteger);
			count++;
		}
		for(int i=0;i<numOfReal;i++){
			attrType[count] = new AttrType(AttrType.attrReal);
			count++;
		}
		
		return attrType;
	}
	
	
	//created: kushal
	public FldSpec[] projections(int offSetCount){
		FldSpec[] projections = new FldSpec[offSetCount];
		for (int i = 0; i < offSetCount; i++) {
			projections[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
		}
		return projections;
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

	private String getFileName(String fileName) {
		String string[] = fileName.split("\\.");
		StringBuilder builder = new StringBuilder();
		builder.append(string[0]).append(".in");
		return builder.toString();

	}

}
