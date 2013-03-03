package iterator;

import heap.*;
import global.*;
import diskmgr.*;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.IndexFile;
import btree.IndexFileScan;
import btree.IntegerKey;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import bufmgr.*;
import index.*;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

public class TopRankJoin extends Iterator implements GlobalConst {

	public ArrayList<Object> Result = new ArrayList<Object>();
	public HashSet<Object> keyset = new HashSet<Object>();
	private int _n_pages;
	private PageId[] bufs_pids;
	private byte[][] bufs;
	private boolean useBM = true;
	static int scanTuple = 0;
	static int probeTuple = 0;
	private static Iterator p_i;
	private static ArrayList<HeapElement>finalResult;
	private static int Top_k ;
	 private static AttrType[][] inAttrType;
	 private static int[] len_col;
	 private static short[][] str_size;
	 private Heapfile temp_file_fd1;//temporary heap  file TASK 3
	 Iterator[] am;
	/*******
	 * 
	 * @param numTables
	 * @param in
	 * @param len_in
	 * @param s_size
	 * @param join_col_in
	 * @param am
	 * @param index
	 * @param indNames
	 * @param amt_of_mem
	 * @param outFilter
	 * @param proj_list
	 * @param n_out_flds
	 * @param num
	 * @param s_size
	 * @param len_in 
	 * @throws JoinsException
	 * @throws IndexException
	 * @throws InvalidTupleSizeException
	 * @throws InvalidTypeException
	 * @throws PageNotReadException
	 * @throws TupleUtilsException
	 * @throws PredEvalException
	 * @throws SortException
	 * @throws LowMemException
	 * @throws UnknowAttrType
	 * @throws UnknownKeyTypeException
	 * @throws Exception
	 */

	/*
	 * public TopRankJoin(int numTables, AttrType[][] in, int[] len_in,
	 * short[][] s_size, int[] join_col_in, Iterator[] am, IndexType[] index,
	 * java.lang.String[] indNames, int amt_of_mem, CondExpr[] outFilter,
	 * FldSpec[] proj_list, int n_out_flds, int num) throws JoinsException,
	 * IndexException, InvalidTupleSizeException, InvalidTypeException,
	 * PageNotReadException, TupleUtilsException, PredEvalException,
	 * SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException,
	 * Exception {
	 */
	 Heapfile temp1;
	public TopRankJoin(int numTables, int[] join_col_in, Iterator[] _am,
			int num, FileIndex[] BTfile, short[][] s_size, AttrType[][] in,
			int rank,int n_out_flds, int[] len_in, int amt_of_mem) throws JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, TupleUtilsException, PredEvalException,
			SortException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, Exception {
		// / matchkey is the candidate key
		inAttrType=in;
		len_col=len_in;
		str_size=s_size;
		Top_k = num;
		am = _am;
		 temp1 = new Heapfile("tempfile.in");
		
		AttrType[] attrType = new AttrType[numTables+1];
		
		attrType[0] = new AttrType(AttrType.attrString);
		
		for(int i=1;i<numTables+1;i++){
			attrType[i] = new AttrType(AttrType.attrInteger);
			
		}
		short[] Tsizes = new short[1];
		Tsizes[0] = 30; // first elt. is 30
		Tuple temptuple = new Tuple();
		
		temptuple.setHdr((short) (numTables+1), attrType, Tsizes);
		int size = temptuple.size();
		temptuple= new Tuple(size);
		temptuple.setHdr((short) (numTables+1), attrType, Tsizes);
		
		
		BTreeFile btf = null;
		
		int incr = (short) (Tsizes[0] + 2); // strlen in bytes =
		// strlen +2
		btf = new BTreeFile("TempIndex", AttrType.attrString, incr, 1);
		
		
		HashMap<Object, Map<Integer, Integer>> matchkey = new HashMap<Object, Map<Integer, Integer>>();
	
		
		// /test assume BTree Scan

		Scan fscan = null;
	
		RID rid = new RID();
		BTFileScan btscan;
		KeyDataEntry temp_scan;

		while (true) {
			
			for (int i = 0; i < numTables; i++) {

				Tuple tt = null;
				String key;
				int col = join_col_in[i];
				if ((tt = am[i].get_next()) != null) {

					AttrType[] Stypes = in[i];
					short[] Ssizes = s_size[i];
					tt.setHdr((short) len_in[i], Stypes, Ssizes);
					key = tt.getStrFld(col);
			
					//System.out.println("key is " + key);
					scanTuple ++;
					
					
					KeyClass lowkey = new StringKey((String) key);
					KeyClass hikey = new StringKey((String) key);
					
					btscan = btf.new_scan(lowkey, hikey);
					Tuple ttuple = null;
					
					
					if((temp_scan = btscan.get_next()) != null)
					{
				
						rid = ((LeafData) temp_scan.data).getData();
					//	System.out.println("if rid " + rid);
						temptuple = temp1.getRecord(rid);
					//	temptuple= new Tuple(tsize);
						temptuple.setHdr((short) (numTables+1), attrType, Tsizes);
					//	temptuple.print(attrType); System.out.println("+" + temptuple.getIntFld(4));
					
						int tsize = temptuple.size();
						
						
					//	System.out.println(temptuple);
						int counter = temptuple.getIntFld(i+2);
						temptuple.setIntFld(i+2, counter+1);
						temp1.updateRecord(rid, temptuple);
					}
					else
					{
				
						temptuple.setStrFld(1, key);
						for(int j =2; j< numTables+2; j++)
						{
							temptuple.setIntFld(j, 0);
						}
						temptuple.setIntFld(i+2, 1);
			
						rid = temp1.insertRecord(temptuple.returnTupleByteArray());
					
						
						
						btf.insert(new StringKey(key.toString()), rid);
					//	System.out.println("else rid " + rid);
						
					}
			
				}

			}
			//System.out.println("finished");

			
			
			
			Tuple checktuple = null;
			
			FldSpec[] Sprojection = new FldSpec[4];
			for (int j = 0; j < 4; j++) {
				Sprojection[j] = new FldSpec(new RelSpec(RelSpec.outer), j+1);
			}
			int totalnum = 0;
			FileScan scan = new FileScan("tempfile.in",attrType, Tsizes,(short)(numTables+1),(numTables+1),Sprojection,null);
			while((checktuple = scan.get_next())!= null)
			{
			
				int checknum = 1;
				for(int t =0; t< numTables ; t++)
				{
					checknum *= checktuple.getIntFld(t+2);
				}
				totalnum+=checknum;
			
			}
			
			
			if(totalnum>=num)
				break;
			
			
		}

		// /Begin random access

	

		// /////// fake name
		//KeyDataEntry temp_scan;
		Tuple testuple = null;
		//HashMap<Object, HashMap<Integer, ArrayList<Element>>> CandidateResult = new HashMap<Object, HashMap<Integer, ArrayList<Element>>>();
		
		HashMap<Integer, ArrayList<Element>> CandidateResult  = new HashMap<Integer, ArrayList<Element>>();
		int totalTuple = 0;
	
		FldSpec[] Sprojection = new FldSpec[4];
		for (int j = 0; j < 4; j++) {
			Sprojection[j] = new FldSpec(new RelSpec(RelSpec.outer), j+1);
		}
		
		FileScan scan = new FileScan("tempfile.in",attrType, Tsizes,(short)(numTables+1),(numTables+1),Sprojection,null);
		while((testuple = scan.get_next())!= null)
		{
			int i = 0;
			String k = testuple.getStrFld(1);
			
			ArrayList<ArrayList<Element>> list = new ArrayList<ArrayList<Element>>();
			
			for (FileIndex o : BTfile) {

				AttrType[] Stypes = in[i];

				short[] Ssizes = s_size[i];

				

				BTreeFile e = o.btindex;
				Heapfile f = o.heapfile;
				BTFileScan hscan;
				KeyClass lowkey = new StringKey((String) k);
				KeyClass hikey = new StringKey((String) k);

				

				if (i == 0) {
					hscan = e.new_scan(lowkey, hikey);
					while ((temp_scan = hscan.get_next()) != null) {
						rid = ((LeafData) temp_scan.data).getData();	
						testuple = f.getRecord(rid);
						testuple.setHdr((short) len_in[i], Stypes, Ssizes);

						Element tempelement = new Element(testuple.getScore(),
								testuple);

						ArrayList<Element> al3 = new ArrayList<Element>();
						al3.add(tempelement);
						list.add(al3);
						totalTuple++;

						// System.out.println(" al3 " + al3.size());
					}
					//System.out.println(" in if " + list.size());
				} else {
					int list_size = list.size();
					for (int j = 0; j < list_size; j++) {
						//int t =0;
						hscan = e.new_scan(lowkey, hikey);
						while ((temp_scan = hscan.get_next()) != null) {
							rid = ((LeafData) temp_scan.data).getData();
							// float Score = getTupleScore(rid, ioBuf, f);
							testuple = f.getRecord(rid);
							testuple.setHdr((short) len_in[i], Stypes, Ssizes);

							Element tempelement = new Element(
									testuple.getScore(), testuple);
							ArrayList<Element> al3 = new ArrayList<Element>();
							al3.addAll(list.get(j));
							al3.add(tempelement);
							list.add(al3);
							if(j==0)
							totalTuple++;
							//t++;
						//	System.out.println(t + " times " + list.size());
						}
					}
					
			
					for(int j = 0 ; j< list_size; j++)
					{
						list.remove(0);
					}
				//	System.out.println(" else  " + list.size());
				}
				
			//	System.out.println(" out loop " + i + " ~~ " + list.size());
				i++;
				
			}
			
			Tuple resulttuple = null;
			Tuple ttt = null;
		//	System.out.println(" out side " + list.size());
			for (ArrayList<Element> elementList : list) {

			//	System.out.println(" print " + elementList.size());
				resulttuple = ProjectJoin(elementList);
			//	resulttuple.print(jType);
				ttt = resulttuple;

				int r_size = resulttuple.size();
				resulttuple = new Tuple(r_size);
				resulttuple.tupleCopy(ttt);
				resulttuple.setHdr((short) jType.length, jType, t_size);

				resultheap.insertRecord(resulttuple.returnTupleByteArray());

			}
			list.clear();
			
			
			
		//	FindCombination(CandidateResult, numTables);
		//	CandidateResult.clear();
		
		}
		
		
		
		
		
		

		probeTuple = totalTuple - scanTuple;
		// // begin combination of fielD

		
		
		FldSpec[]Tprojection =new  FldSpec[jType.length];
		for(int x=0;x<Tprojection.length;x++)
		{
			Tprojection[x]= new FldSpec(new RelSpec(RelSpec.outer),x+1);
		}

		
		TupleOrder sortOrder = null;
		
		sortOrder = new TupleOrder(TupleOrder.Descending);
		
		FileScan resultscan = new FileScan("resultheap.in",jType, t_size,(short) jType.length,jType.length ,Tprojection,null);
		
		p_i = new Sort(jType, (short) jType.length, t_size, resultscan,
				jType.length, sortOrder, 4, amt_of_mem);
		
	//	p_i.get_next().print(jType);

	}

	Heapfile resultheap = new Heapfile("resultheap.in");
	
	private Object FindCombination(
			HashMap<Integer, ArrayList<Element>> candidateResult , int numTables) throws TupleUtilsException, UnknowAttrType, FieldNumberOutOfBoundException, InvalidTypeException, InvalidTupleSizeException, IOException, InvalidSlotNumberException, SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException {

		Tuple resulttuple = null;
		HashMap<Integer, ArrayList<Element>> KeyTableinfo = candidateResult;

		if (KeyTableinfo.size() < numTables)
			return null;
		ArrayList<ArrayList<Element>> list = new ArrayList<ArrayList<Element>>();

		list = findList(numTables, KeyTableinfo, list);

		Tuple ttt = null;

		for (ArrayList<Element> elementList : list) {

			resulttuple = ProjectJoin(elementList);
			ttt = resulttuple;

			int r_size = resulttuple.size();
			resulttuple = new Tuple(r_size);
			resulttuple.tupleCopy(ttt);
			resulttuple.setHdr((short) jType.length, jType, t_size);

			resultheap.insertRecord(resulttuple.returnTupleByteArray());

		}
		return null;
	
		
		// TODO Auto-generated method stub
		
	}

	private static FldSpec[] proj;

	private Tuple ProjectJoin(ArrayList<Element> elementList) throws TupleUtilsException, IOException, UnknowAttrType, FieldNumberOutOfBoundException, InvalidTypeException, InvalidTupleSizeException {
		// TODO Auto-generated method stub
		
		
		ArrayList<Element> test = elementList;
		if (test.size() < 2) {
		System.out.println("error, less than two tables");
		return null;
		}
		
	/*	
		 AttrType [] Rtypes = new AttrType[3];
		    Rtypes[0] = new AttrType (AttrType.attrString);
		    Rtypes[1] = new AttrType (AttrType.attrString);
		    Rtypes[2] = new AttrType (AttrType.attrReal);*/
		    
		Tuple temp1 = test.get(0).tuple;
	//	temp1.print(Rtypes);
		Tuple temp2 = test.get(1).tuple;
	//	temp2.print(Rtypes);
		Tuple Jtuple = new Tuple();
		// create proj_list
		int proj_len = len_col[0] + len_col[1] - 2;
		proj = new FldSpec[proj_len];
		for (int i = 0; i < len_col[0] - 1; i++) {
		proj[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
		}
		for (int i = 0; i < len_col[1] - 1; i++) {
		proj[i + len_col[0] - 1] = new FldSpec(
		new RelSpec(RelSpec.innerRel), i + 1);
		}
		// create jtype
		jType = new AttrType[proj_len+1];
		/*for (int i = 0; i < proj_len; i++) {
		jType[i] = new AttrType(AttrType.attrString);
		}*/
		t_size = TupleUtils.setup_op_tuple(Jtuple, jType,
		inAttrType[0], len_col[0], inAttrType[1], len_col[1],
		str_size[0], str_size[1], proj, proj_len,true);
		Projection.Join(temp1, inAttrType[0], temp2, inAttrType[1], Jtuple,
		proj, proj_len,true);
		//Jtuple.print(jType);////////////
		for (int i = 2; i < test.size(); i++) {
		short[] laststr_size = t_size;
		AttrType[] lastType = jType;
		int lastres = jType.length;
		Tuple temp3 = test.get(i).tuple;
		proj_len = proj_len + len_col[i] - 1;
		Tuple lasttuple = new Tuple();// =Jtuple;
		lasttuple.setHdr((short) lastres, lastType, laststr_size);
		lasttuple.tupleCopy(Jtuple);
		// create proj;
		proj = new FldSpec[proj_len];
		for (int j = 0; j < lastres-1; j++) {
		proj[j] = new FldSpec(new RelSpec(RelSpec.outer), j + 1);
		}
		for (int j = 0; j < len_col[i] - 1; j++) {
		proj[j + lastres-1] = new FldSpec(new RelSpec(RelSpec.innerRel),
		j + 1);
		}
		jType = new AttrType[proj_len+1];
		t_size = TupleUtils.setup_op_tuple(Jtuple, jType, lastType,
		lastres, inAttrType[i], len_col[i], laststr_size,
		str_size[i], proj, proj_len,true);
		Projection.Join(lasttuple, lastType, temp3, inAttrType[i], Jtuple,
		proj, proj_len,true);
		
		
		
		
		
		}
		counter++;
		
		return Jtuple;
		
	}



	public ArrayList<ArrayList<Element>> findList(int tableNum,
			HashMap<Integer, ArrayList<Element>> keyTable,
			ArrayList<ArrayList<Element>> list) {

		if (tableNum > 0) {
			list = createCombination(keyTable.get(tableNum - 1), list);

			list = findList(tableNum - 1, keyTable, list);
		} else {
			return list;
		}
		return list;
	}

	public static void main(String args[]) throws Exception {

		String[] tablename = { "table1.in", "table2.in", "table3.in" };
		AttrType[] Stypes = new AttrType[3];
		Stypes[0] = new AttrType(AttrType.attrString);
		Stypes[1] = new AttrType(AttrType.attrString);
		Stypes[2] = new AttrType(AttrType.attrReal);
		short[] Ssizes = new short[2];
		Ssizes[0] = 30; // first elt. is 30
		Ssizes[1] = 30;
		AttrType[][] in = { Stypes, Stypes, Stypes };
		short[][] s_size = { Ssizes, Ssizes, Ssizes };
		int[] len_in = { 3, 3, 3 };
		int numTables = 3;
		int rank = 1;
		int TopK = 3;
		String[] indNames = { "BtreeIndex1", "BtreeIndex2", "BtreeIndex3" };
		int[] join_col = { 1, 1, 1 };
		int n_out_flds = 3;

		
		
		
		TopRankBuildIndex testtoprank = new TopRankBuildIndex();
		System.out.println("**************get FileIndex***************");
		FileIndex[] BTfile = testtoprank.getFileIndex(s_size, in, len_in,
				numTables, tablename, rank, indNames, join_col, n_out_flds);

		System.out.println("**************get SortedIterator ***************");
		Iterator[] am = testtoprank.getSortedIterator(s_size, in, len_in,
				numTables, tablename, rank,n_out_flds);
		TopRankJoin test = new TopRankJoin(numTables, join_col, am, TopK, BTfile, s_size,
				in, rank , n_out_flds, len_in, 10);
		
		close( am, BTfile);
		System.out.println(" Num of Scaned Tuple is " + test.num_scanned(1));
		System.out.println(" Num of Probe Tuple is " + test.num_probed(1));
		Tuple t=new Tuple();
		while((t=test.get_next())!=null)
		{
			t.print(test.jType);
			System.out.println(t.getScore());
		}
		

	}

	public double roundTwoDecimals(double d) {
		DecimalFormat twoDForm = new DecimalFormat("#.####");
		return Double.valueOf(twoDForm.format(d));
	}

	public ArrayList<ArrayList<Element>> createCombination(
			ArrayList<Element> al1, ArrayList<ArrayList<Element>> al2) {
		ArrayList<ArrayList<Element>> joinList = new ArrayList<ArrayList<Element>>();
		// System.out.println(al1.size()+ " **** test *** " + al2.size());
		for (int i = 0; i < al1.size(); i++) {
			if (al2.size() != 0) {
				for (int j = 0; j < al2.size(); j++) {
					ArrayList<Element> al3 = new ArrayList<Element>();
					al3.add(al1.get(i));
					al3.addAll(al2.get(j));
					// System.out.println(al3.size() + " al3 size");
					joinList.add(al3);
				}

			} else {
				ArrayList<Element> al3 = new ArrayList<Element>();
				al3.add(al1.get(i));
				// al3.addAll(al2.get(j));
				joinList.add(al3);
			}
		}
		// System.out.println(" join list size is " + joinList.size());
		return joinList;
	}

	public int num_scanned(int in_rel)
	{
		
		return scanTuple;	
	}
	
	public int num_probed(int in_rel)
	{
		return probeTuple;
	}
	

	
	
	public  AttrType[] jType;
	private static short[] t_size;
	private static int counter = 0;


	public Tuple get_next() throws JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, TupleUtilsException, PredEvalException,
			SortException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, IOException, Exception {

		if (Top_k-- == 0) {
			System.out.println("reach end");
			return null;
		}

		return p_i.get_next();

	}

	public static void close(Iterator[] am, FileIndex[] bTfile) throws IOException, JoinsException, SortException,
			IndexException, InvalidSlotNumberException, FileAlreadyDeletedException, InvalidTupleSizeException, HFBufMgrException, HFDiskMgrException {
		// TODO Auto-generated method stub
		for( Iterator a : am)
		{
			a.close();
		}
		for(FileIndex b : bTfile)
		{
			b.heapfile.deleteFile();
		}

	}

	@Override
	public void close() throws IOException, JoinsException, SortException,
			IndexException {
		if (!closeFlag) {
						
			try {
				resultheap.deleteFile();
				temp1.deleteFile();
				
			} catch (InvalidSlotNumberException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (FileAlreadyDeletedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (InvalidTupleSizeException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (HFBufMgrException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (HFDiskMgrException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			

			try {
				}catch (Exception e) {
					throw new JoinsException(e, "TopRankJoin.java: error in closing iterator.");
				}
			}
		closeFlag = true;
		
	}

}
