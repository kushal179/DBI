package iterator;

import java.io.IOException;
import java.util.Vector;

import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import global.TupleOrder;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.Sort;

import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;




import btree.BTFileScan;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.IteratorException;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.KeyNotMatchException;
import btree.LeafData;
import btree.PinPageException;
import btree.ScanIteratorException;
import btree.UnpinPageException;

import btree.IntegerKey;

import btree.StringKey;

import bufmgr.PageNotReadException;

class table1 {
	  public String    att1;
	  public String att2;
	  public float    score;
	  public table1 (String _att1, String _att2, float _score) {
	    att1    = _att1;
	    att2  = _att2;
	    score=_score;
	  }
	}
class table2 {
	  public String    att1;
	  public String att2;
	  public float    score;
	  public table2 (String _att1, String _att2, float _score) {
	    att1    = _att1;
	    att2  = _att2;
	    score=_score;
	  }
	}
class table3 {
	  public String    att1;
	  public String att2;
	  public float    score;
	  public table3 (String _att1, String _att2, float _score) {
	    att1    = _att1;
	    att2  = _att2;
	    score=_score;
	  }
	}














public class TopRankBuildIndex implements GlobalConst {
	private static boolean OK = true;
	private static boolean FAIL = false;
	

	 
	public static RID[] ridfors;
	
	public static void test(){
	
	
		
	}
	
	
	
	
	
	
	

	public TopRankBuildIndex() {
		
	/*	Vector table1;
		 Vector table2;
		 Vector table3;
		    //build Sailor, Boats, Reserves table
		    table1  = new Vector();
		    table2  = new Vector();
		    table3  = new Vector();
		    
		    table1.addElement(new table1("a","1",(float)0.9));
		    table1.addElement(new table1("f","16",(float)0.2));
		    table1.addElement(new table1("b","4",(float)0.8));
		    table1.addElement(new table1("b","14",(float)0.3));
		    table1.addElement(new table1("e","7",(float)0.5));
		    table1.addElement(new table1("c","10",(float)0.5));
		  

		    table2.addElement(new table2("b","2",(float)0.8));
		    table2.addElement(new table2("c","5",(float)0.7));
		    table2.addElement(new table2("a","8",(float)0.6));
		    table2.addElement(new table2("d","17",(float)0.2));
		    table2.addElement(new table2("c","14",(float)0.3));
		    table2.addElement(new table2("b","11",(float)0.4));
		    
		    table3.addElement(new table3("d","18",(float)0.1));
		    table3.addElement(new table3("b","15",(float)0.2));
		    table3.addElement(new table3("a","12",(float)0.3));
		    table3.addElement(new table3("c","9",(float)0.5));
		    table3.addElement(new table3("f","6",(float)0.7));
		    table3.addElement(new table3("a","3",(float)0.9));
		  

		    boolean status = OK;
		    int numsailors = 6;
		    int numsailors_attrs = 3;
		    int numreserves = 6;
		    int numreserves_attrs = 3;
		    int numboats = 6;
		    int numboats_attrs = 3;
		    
		    String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
		    String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

		    String remove_cmd = "/bin/rm -rf ";
		    String remove_logcmd = remove_cmd + logpath;
		    String remove_dbcmd = remove_cmd + dbpath;
		    String remove_joincmd = remove_cmd + dbpath;

		    try {
		      Runtime.getRuntime().exec(remove_logcmd);
		      Runtime.getRuntime().exec(remove_dbcmd);
		      Runtime.getRuntime().exec(remove_joincmd);
		    }
		    catch (IOException e) {
		      System.err.println (""+e);
		    }

		   
		    
		    ExtendedSystemDefs extSysDef = 
		      new ExtendedSystemDefs( "/tmp/minibase.jointestdb", "/tmp/joinlog",
					      1000,500,200,"Clock");
		    

		    SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );
		    
		    // creating the sailors relation
		    AttrType [] Stypes = new AttrType[3];
		    Stypes[0] = new AttrType (AttrType.attrString);
		    Stypes[1] = new AttrType (AttrType.attrString);
		    Stypes[2] = new AttrType (AttrType.attrReal);
		    
		    //SOS
		    short [] Ssizes = new short [2];
		    Ssizes[0] = 30; //first elt. is 30
		    Ssizes[1] = 30;
		    
		    Tuple t = new Tuple();
		    try {
		      t.setHdr((short) 3,Stypes, Ssizes);
		    }
		    catch (Exception e) {
		      System.err.println("*** error in Tuple.setHdr() ***");
		      status = FAIL;
		      e.printStackTrace();
		    }
		    
		    int size = t.size();
		    
		    // inserting the tuple into file "sailors"
		    //ridfors=new RID[numsailors];
		    RID             rid;
		    Heapfile        f = null;
		    try {
		      f = new Heapfile("table1.in");
		    }
		    catch (Exception e) {
		      System.err.println("*** error in Heapfile constructor ***");
		      status = FAIL;
		      e.printStackTrace();
		    }
		    
		    t = new Tuple(size);
		    try {
		      t.setHdr((short) 3, Stypes, Ssizes);
		    }
		    catch (Exception e) {
		      System.err.println("*** error in Tuple.setHdr() ***");
		      status = FAIL;
		      e.printStackTrace();
		    }
		    
		    for (int i=0; i<numsailors; i++) {
		      try {
			t.setStrFld(1, ((table1)table1.elementAt(i)).att1);
			t.setStrFld(2, ((table1)table1.elementAt(i)).att2);
			t.setFloFld(3, ((table1)table1.elementAt(i)).score);
			
		      }
		      catch (Exception e) {
			System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
			status = FAIL;
			e.printStackTrace();
		      }
		      
		      try {
			rid = f.insertRecord(t.returnTupleByteArray());
			//ridfors[i]=rid;
			
		      }
		      catch (Exception e) {
			System.err.println("*** error in Heapfile.insertRecord() ***");
			status = FAIL;
			e.printStackTrace();
		      }      
		    }
		    if (status != OK) {
		      //bail out
		      System.err.println ("*** Error creating relation for sailors");
		      Runtime.getRuntime().exit(1);
		    }
		    
		    //creating the boats relation
		    AttrType [] Btypes = {
		     
		      new AttrType(AttrType.attrString), 
		      new AttrType(AttrType.attrString), 
		      new AttrType(AttrType.attrReal), 
		    };
		    
		    short  []  Bsizes = new short[2];
		    Bsizes[0] = 30;
		    Bsizes[1] = 30;
		    t = new Tuple();
		    try {
		      t.setHdr((short) 3,Btypes, Bsizes);
		    }
		    catch (Exception e) {
		      System.err.println("*** error in Tuple.setHdr() ***");
		      status = FAIL;
		      e.printStackTrace();
		    }
		    
		    size = t.size();
		    
		    // inserting the tuple into file "boats"
		    //RID             rid;
		    f = null;
		    try {
		      f = new Heapfile("table2.in");
		    }
		    catch (Exception e) {
		      System.err.println("*** error in Heapfile constructor ***");
		      status = FAIL;
		      e.printStackTrace();
		    }
		    
		    t = new Tuple(size);
		    try {
		      t.setHdr((short) 3, Btypes, Bsizes);
		    }
		    catch (Exception e) {
		      System.err.println("*** error in Tuple.setHdr() ***");
		      status = FAIL;
		      e.printStackTrace();
		    }
		    
		    for (int i=0; i<numboats; i++) {
		      try {
			
			t.setStrFld(1, ((table2)table2.elementAt(i)).att1);
			t.setStrFld(2, ((table2)table2.elementAt(i)).att2);
			t.setFloFld(3, ((table2)table2.elementAt(i)).score);
		      }
		      catch (Exception e) {
			System.err.println("*** error in Tuple.setStrFld() ***");
			status = FAIL;
			e.printStackTrace();
		      }
		      
		      try {
			rid = f.insertRecord(t.returnTupleByteArray());
		      }
		      catch (Exception e) {
			System.err.println("*** error in Heapfile.insertRecord() ***");
			status = FAIL;
			e.printStackTrace();
		      }      
		    }
		    if (status != OK) {
		      //bail out
		      System.err.println ("*** Error creating relation for boats");
		      Runtime.getRuntime().exit(1);
		    }
		    
		    //creating the boats relation
		    AttrType [] Rtypes = new AttrType[3];
		    Rtypes[0] = new AttrType (AttrType.attrString);
		    Rtypes[1] = new AttrType (AttrType.attrString);
		    Rtypes[2] = new AttrType (AttrType.attrReal);

		    short [] Rsizes = new short [2];
		    Rsizes[0] = 30; 
		    Rsizes[1] = 30; 
		    t = new Tuple();
		    try {
		      t.setHdr((short) 3,Rtypes, Rsizes);
		    }
		    catch (Exception e) {
		      System.err.println("*** error in Tuple.setHdr() ***");
		      status = FAIL;
		      e.printStackTrace();
		    }
		    
		    size = t.size();
		    
		    // inserting the tuple into file "boats"
		    //RID             rid;
		    f = null;
		    try {
		      f = new Heapfile("table3.in");
		    }
		    catch (Exception e) {
		      System.err.println("*** error in Heapfile constructor ***");
		      status = FAIL;
		      e.printStackTrace();
		    }
		    
		    t = new Tuple(size);
		    try {
		      t.setHdr((short) 3, Rtypes, Rsizes);
		    }
		    catch (Exception e) {
		      System.err.println("*** error in Tuple.setHdr() ***");
		      status = FAIL;
		      e.printStackTrace();
		    }
		    
		    for (int i=0; i<numreserves; i++) {
		      try {
			t.setStrFld(1, ((table3)table3.elementAt(i)).att1);
			t.setStrFld(2, ((table3)table3.elementAt(i)).att2);
			t.setFloFld(3, ((table3)table3.elementAt(i)).score);

		      }
		      catch (Exception e) {
			System.err.println("*** error in Tuple.setStrFld() ***");
			status = FAIL;
			e.printStackTrace();
		      }      
		      
		      try {
			rid = f.insertRecord(t.returnTupleByteArray());
		      }
		      catch (Exception e) {
			System.err.println("*** error in Heapfile.insertRecord() ***");
			status = FAIL;
			e.printStackTrace();
		      }      
		    }
		    if (status != OK) {
		      //bail out
		      System.err.println ("*** Error creating relation for reserves");
		      Runtime.getRuntime().exit(1);
		    }*/

	}

	// /problem????????????????????????????
	@SuppressWarnings("null")
	public  Iterator[] getSortedIterator(short[][] s_size, AttrType[][] in,
			int[] len_in, int numTables, String[] tablename, int rank, int n_out_flds)
			throws JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException,
			PredEvalException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, Exception {
		boolean status = OK;

		FileScan am = null;

		Iterator[] p_am = new Iterator[numTables];

		TupleOrder sortOrder = null;

		// ///problem????????????????????????????????
		switch (rank) {
		case 0:

			sortOrder = new TupleOrder(TupleOrder.Ascending);
			break;

		case 1:

			sortOrder = new TupleOrder(TupleOrder.Descending);
			break;

		}

		for (int i = 0; i < numTables; i++) {

			AttrType[] Stypes = in[i];
			short[] Ssizes = s_size[i];
			FldSpec[] Sprojection = new FldSpec[len_in[i]];
			for (int j = 0; j < len_in[i]; j++) {
				Sprojection[j] = new FldSpec(new RelSpec(RelSpec.outer), j+1);
			}
			
			try {
				am = new FileScan(tablename[i], Stypes, Ssizes, (short) len_in[i], len_in[i],
						Sprojection, null);

			} catch (Exception e) {
				status = FAIL;
				System.err.println("" + e);
			}
			Iterator p_i = new Sort(Stypes, (short) Stypes.length, Ssizes, am,
					Stypes.length, sortOrder, 4, 10);
			p_am[i] = p_i;
		}
		return p_am;

	}

	public  FileIndex[] getFileIndex(short[][] s_size, AttrType[][] in,
			int[] len_in, int numTables, String[] tablename, int rank,
			java.lang.String[] indNames, int[] join_col , int n_out_flds) throws Exception {

		FileIndex BTfile[] = new FileIndex[numTables];
		

		for (int i = 0; i < numTables; i++) {

			AttrType[] Stypes = in[i];
			short[] Ssizes = s_size[i];
			Heapfile f = new Heapfile(tablename[i]);// /// tablename[i] is
						// "table1.in"
			createindex(Stypes.length, Stypes, Ssizes, tablename[i],
					indNames[i], join_col[i]);

			BTreeFile file = new BTreeFile(indNames[i]);
			
			BTfile[i] = new FileIndex(f, file);
		
			//printout(BTfile[i]);
		}
		return BTfile;
	}

	public static void main(String args[]) throws Exception {
		TopRankBuildIndex testtoprank = new TopRankBuildIndex();
		
		String[] tablename = {"table1.in","table2.in","table3.in"};
		AttrType[] Stypes = new AttrType[3];
		Stypes[0] = new AttrType(AttrType.attrString);
		Stypes[1] = new AttrType(AttrType.attrString);
		Stypes[2] = new AttrType(AttrType.attrReal);
		short[] Ssizes = new short[2];
		Ssizes[0] = 30; // first elt. is 30
		Ssizes[1] = 30;
		AttrType[][] in = {Stypes,Stypes,Stypes};
		short[][]  s_size = {Ssizes,Ssizes,Ssizes};
		int [] len_in = {3,3,3};
		int numTables =3;
		int rank = 1;
		int [] join_col_in={1,1,1};
		String[] indNames = {"BtreeIndex1","BtreeIndex2","BtreeIndex3"};
		int n_out_flds =3;
		//test();

		System.out.println("*****************************");
		FileIndex[] BTfile = testtoprank.getFileIndex(s_size, in, len_in, 3, tablename, rank, indNames,join_col_in, n_out_flds);
		System.out.println(BTfile.length);
		System.out.println("*****************************");
	
		for ( FileIndex e : BTfile)
		{
			
			printout(e);
		
		}
		System.out.println("***********");
		Iterator[] am = testtoprank.getSortedIterator(s_size, in, len_in, 3, tablename, rank, n_out_flds);
		for (Iterator a : am)
		{
			System.out.println("~~~~~~~~~~~~~");
			printsortout(a);
		}
		System.out.println(am.length);
	}

	private static void printsortout(Iterator am) throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		
		  AttrType [] Btypes = {
			   	     
		  	      new AttrType(AttrType.attrString), 
		  	      new AttrType(AttrType.attrString), 
		  	      new AttrType(AttrType.attrReal), 
		  	    };
		  	    
		  	    short  []  Bsizes = new short[2];
		  	    Bsizes[0] = 30;
		  	    Bsizes[1] = 30;
		  	  Tuple testtuple;
		// TODO Auto-generated method stu
		    while((testtuple = am.get_next())!=null)
			{
		    	testtuple.setHdr((short)3,Btypes, Bsizes);
		    	testtuple.print(Btypes);
			}
		
	}








	private static void printout(FileIndex e) throws ScanIteratorException, InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, InvalidTypeException, Exception {
		// TODO Auto-generated method stub
		
		 AttrType [] Stypes = new AttrType[3];
		    Stypes[0] = new AttrType (AttrType.attrString);
		    Stypes[1] = new AttrType (AttrType.attrString);
		    Stypes[2] = new AttrType (AttrType.attrReal);
		    short [] Ssizes = new short [2];
		    Ssizes[0] = 30; //first elt. is 30
		    Ssizes[1] = 30;
			 
	    KeyDataEntry temp_scan;
	    RID rid=new RID();
	    Tuple testtuple;
	   //try table1.in
	    Heapfile f = e.heapfile;
	    BTreeFile file = e.btindex;
		KeyClass lowkey = new StringKey("a");
		KeyClass hikey = new StringKey("b");
		BTFileScan scan = file.new_scan(lowkey, hikey);
		while((temp_scan = scan.get_next())!=null)
		{
			rid=((LeafData)temp_scan.data).getData();
			//System.out.println(rid);
			testtuple=f.getRecord(rid);
			//System.out.println(testtuple);
			testtuple.setHdr((short)3, Stypes, Ssizes);
			 System.out.println("SCAN RESULT: "+ temp_scan.key);
			 testtuple.print(Stypes);			 
			
		}
		
		
	}








	/**
	 * 
	 * @param len
	 *            the length of attribute
	 * @param Stypes
	 *            attribute type
	 * @param Ssizes
	 *            string size
	 * @param filename
	 *            heapfile name eg sailors.in
	 * @param indexname
	 *            index name
	 * @param col
	 *            the column number to build the index on
	 */
	public static void createindex(int len, AttrType[] Stypes, short[] Ssizes,
			String filename, String indexname, int col) {
		boolean status = OK;

		// _______________________________________________________________
		// *******************create an scan on the heapfile**************
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// create a tuple of appropriate size
		Tuple tt = new Tuple();
		try {
			tt.setHdr((short) len, Stypes, Ssizes);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		int sizett = tt.size();
		tt = new Tuple(sizett);
		try {
			tt.setHdr((short) len, Stypes, Ssizes);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		Heapfile f = null;
		try {
			f = new Heapfile(filename);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		Scan scan = null;

		try {
			scan = new Scan(f);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		// create the index file,some problem with str case
		short strCount = 0;
		BTreeFile btf = null;
		try {
			switch (Stypes[col - 1].attrType) {

			case AttrType.attrInteger:

				btf = new BTreeFile(indexname, AttrType.attrInteger, 4, 1);
				break;

			case AttrType.attrReal:
				btf = new BTreeFile(indexname, AttrType.attrReal, 4, 1);
				break;

			case AttrType.attrString:
				int incr = (short) (Ssizes[strCount] + 2); // strlen in bytes =
															// strlen +2
				btf = new BTreeFile(indexname, AttrType.attrString, incr, 1);
				strCount++;
				break;

			default:
				throw new InvalidTypeException(null, "TUPLE: TUPLE_TYPE_ERROR");
			}
		} catch (Exception e) {
			// status = FAIL;
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		RID rid = new RID();
		Object key = null;
		Tuple temp = null;

		try {
			temp = scan.getNext(rid);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		while (temp != null) {
			tt.tupleCopy(temp);

			try {
				switch (Stypes[col - 1].attrType) {

				case AttrType.attrInteger:
					key = tt.getIntFld(col);
					btf.insert(new IntegerKey((Integer) key), rid);

					break;

				case AttrType.attrReal:
					key = tt.getFloFld(col);
					btf.insert(new StringKey(key.toString()), rid);
					break;

				case AttrType.attrString:
					key = tt.getStrFld(col);
					btf.insert(new StringKey(key.toString()), rid);

					break;
				default:
					throw new InvalidTypeException(null,
							"TUPLE: TUPLE_TYPE_ERROR");
				}
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}

			try {
				temp = scan.getNext(rid);

			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}

		}

		// close the file scan
		scan.closescan();
	}
}
