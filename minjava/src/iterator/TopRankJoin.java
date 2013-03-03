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
	
	private static ArrayList<HeapElement>finalResult;
	 private static AttrType[][] inAttrType;
	 private static int[] len_col;
	 private static short[][] str_size;
	 private Heapfile temp_file_fd1;//temporary heap  file TASK 3
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

	public TopRankJoin(int numTables, int[] join_col_in, Iterator[] am,
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
		HashMap<Object, Map<Integer, Integer>> matchkey = new HashMap<Object, Map<Integer, Integer>>();

		// /test assume BTree Scan
		
		BTreeFile[] btf = null;

		while (true) {
			for (int i = 0; i < numTables; i++) {

				Tuple tt = null;
				Object key;
				int col = join_col_in[i];
				if ((tt = am[i].get_next()) != null) {

					key = tt.getStrFld(col);
					//System.out.println("key is " + key);
					scanTuple ++;
					if (!matchkey.containsKey(key)) {
						int Tnum = i;
						Map<Integer, Integer> temp = new HashMap<Integer, Integer>();
						temp.put(Tnum, 1);
						matchkey.put(key, temp);
					} else {
						if (!matchkey.get(key).containsKey(i)) {
							matchkey.get(key).put(i, 1);
						} else {
							int count = matchkey.get(key).get(i) + 1;
							matchkey.get(key).put(i, count);
						}
					}
				}

			}

			int TotalNum = 0;
			java.util.Iterator<Object> CheckIter = matchkey.keySet().iterator();

			while (CheckIter.hasNext()) {
				Object key = CheckIter.next();
				Map<Integer, Integer> Tableinfo = matchkey.get(key);
				keyset.add(key);
				if (Tableinfo.size() == numTables) {

					int ArrangeNum = 1;

					java.util.Iterator<Integer> ArrangeIter = Tableinfo
							.keySet().iterator();
					while (ArrangeIter.hasNext()) {
						Integer count = Tableinfo.get(ArrangeIter.next());
						ArrangeNum *= count;
					}
					TotalNum += ArrangeNum;
				}
			}
		//	System.out.println("TotalNum is " + TotalNum);
			if (TotalNum >= num)
				break;
		}

		// /Begin random access

		RID rid = new RID();
		IoBuf ioBuf = new IoBuf();
		ioBuf.init(bufs, _n_pages, GlobalConst.MINIBASE_PAGESIZE, temp_file_fd1);

		// /////// fake name
		KeyDataEntry temp_scan;
		Tuple testuple = null;
		HashMap<Object, HashMap<Integer, ArrayList<Element>>> CandidateResult = new HashMap<Object, HashMap<Integer, ArrayList<Element>>>();
		int totalTuple = 0;
		
		for (Object k : keyset) {
			int i = 0;
			if (!CandidateResult.containsKey(k)) {
				CandidateResult.put(k,
						new HashMap<Integer, ArrayList<Element>>());
			}
			
			for (FileIndex o : BTfile) {

				AttrType[] Stypes = in[i];

				short[] Ssizes = s_size[i];

				

				BTreeFile e = o.btindex;
				Heapfile f = o.heapfile;
				BTFileScan scan;
				KeyClass lowkey = new StringKey((String) k);
				KeyClass hikey = new StringKey((String) k);

				scan = e.new_scan(lowkey, hikey);

				while ((temp_scan = scan.get_next()) != null) {
					totalTuple ++;
					rid = ((LeafData) temp_scan.data).getData();
					
				//	float Score = getTupleScore(rid, ioBuf, f);
					testuple = f.getRecord(rid);
					testuple.setHdr((short) len_in[i], Stypes, Ssizes);
					if (!CandidateResult.get(k).containsKey(i)) {
						CandidateResult.get(k).put(i, new ArrayList<Element>());
						CandidateResult
								.get(k)
								.get(i)
								.add(new Element(testuple.getScore(), testuple));
					} else {
						if (CandidateResult.get(k).get(i).size() < num)
							CandidateResult.get(k).get(i)
									.add(new Element(testuple.getScore(), testuple));
					}
				}
				i++;
			}

		}

		probeTuple = totalTuple - scanTuple;
		// // begin combination of fielD

		Heap heap = null;
		if (rank == 1)
			heap = new MinHeap();
		if (rank == 0)
			heap = new MaxHeap();
		java.util.Iterator<Object> it1 = CandidateResult.keySet().iterator();

		while (it1.hasNext()) {
			Object key = it1.next(); // key is the individual value like A, B,
										// C...
			// keytableinfo has information like T1 ----> {A1,A2....},
			// T2---->{A3, A4}
			HashMap<Integer, ArrayList<Element>> KeyTableinfo = CandidateResult
					.get(key);
			if (KeyTableinfo.size() < numTables)
				continue;
			ArrayList<ArrayList<Element>> list = new ArrayList<ArrayList<Element>>();

			list = findList(numTables, KeyTableinfo, list);
			// list will have A's combination in the first loop and so on.

			// computing the score for every element values
			for (ArrayList<Element> elementList : list) {
				float sum = 0;

				for (Element e : elementList) {
					if(sum == 0)
						sum = e.score;
					else
						sum = (sum + e.score)/2;
					// System.out.println("score is " + e.score);
				}
				// System.out.println("sum is " + sum);
				sum = (float) roundTwoDecimals(sum);
				heap.add(new HeapElement(sum, elementList));
			}
		}

		while (heap.size() > num) {
			heap.remove();
		}
		finalResult=new ArrayList<HeapElement>();
		while (heap.size() != 0) {
			HeapElement ID = heap.remove();
			finalResult.add(ID);
		}

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
		
		test.close( am, BTfile);
		System.out.println(" Num of Scaned Tuple is " + test.num_scanned(1));
		System.out.println(" Num of Probe Tuple is " + test.num_probed(1));
		Tuple t=new Tuple();
		while((t=test.get_next())!=null)
		{
			t.print(jType);
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
	

	
	
	public static AttrType[] jType;

	private static int counter = 0;

	public Tuple get_next() throws TupleUtilsException, IOException,
			UnknowAttrType, FieldNumberOutOfBoundException,
			InvalidTypeException, InvalidTupleSizeException {
		// TODO Auto-generated method stub
		// create proj_list
		if (finalResult.size() <= counter) {
			System.out.println("Reaching end");
			return null;
			}
			ArrayList<Element> test = finalResult.get(counter).heapElement;
			if (test.size() < 2) {
			System.out.println("error, less than two tables");
			return null;
			}
			Tuple temp1 = test.get(0).tuple;
			Tuple temp2 = test.get(1).tuple;
			Tuple Jtuple = new Tuple();
			// create proj_list
			int proj_len = len_col[0] + len_col[1] - 2;
			FldSpec[] proj = new FldSpec[proj_len];
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
			short[] t_size = TupleUtils.setup_op_tuple(Jtuple, jType,
			inAttrType[0], len_col[0], inAttrType[1], len_col[1],
			str_size[0], str_size[1], proj, proj_len,true);
			Projection.Join(temp1, inAttrType[0], temp2, inAttrType[1], Jtuple,
			proj, proj_len,true);
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

	public void close(Iterator[] am, FileIndex[] bTfile) throws IOException, JoinsException, SortException,
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
		// TODO Auto-generated method stub
		
	}

}
