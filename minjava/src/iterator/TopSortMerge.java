package iterator;

import global.AttrType;
import global.GlobalConst;
import global.TupleOrder;
import heap.Heapfile;
import heap.Tuple;

import java.io.IOException;
import java.util.ArrayList;

/**
 * This file contains the interface for the sort_merg joins. We name the two
 * relations being joined as R and S. This file contains an implementation of
 * the sort merge join algorithm as described in the Shapiro paper. It makes use
 * of the external sorting utility to generate runs, and then uses the iterator
 * interface to get successive tuples for the final merge.
 */
public class TopSortMerge extends SortMerge implements GlobalConst {
	private AttrType _in1[], _in2[];
	private Tuple Jtuple;
	ArrayList<Tuple> sortTupleList;
	AttrType[] Jtypes;
	private int nOutFlds;


	/**
	 * constructor,initialization
	 * 
	 * @param in1
	 *            [] Array containing field types of R
	 * @param len_in1
	 *            # of columns in R
	 * @param s1_sizes
	 *            shows the length of the string fields in R.
	 * @param in2
	 *            [] Array containing field types of S
	 * @param len_in2
	 *            # of columns in S
	 * @param s2_sizes
	 *            shows the length of the string fields in S
	 * @param sortFld1Len
	 *            the length of sorted field in R
	 * @param sortFld2Len
	 *            the length of sorted field in S
	 * @param join_col_in1
	 *            The col of R to be joined with S
	 * @param join_col_in2
	 *            the col of S to be joined with R
	 * @param amt_of_mem
	 *            IN PAGES
	 * @param am1
	 *            access method for left input to join
	 * @param am2
	 *            access method for right input to join
	 * @param in1_sorted
	 *            is am1 sorted?
	 * @param in2_sorted
	 *            is am2 sorted?
	 * @param order
	 *            the order of the tuple: assending or desecnding?
	 * @param outFilter
	 *            [] Ptr to the output filter
	 * @param proj_list
	 *            shows what input fields go where in the output tuple
	 * @param n_out_flds
	 *            number of outer relation fileds
	 * @exception JoinNewFailed
	 *                allocate failed
	 * @exception JoinLowMemory
	 *                memory not enough
	 * @exception SortException
	 *                exception from sorting
	 * @exception TupleUtilsException
	 *                exception from using tuple utils
	 * @exception IOException
	 *                some I/O fault
	 */
	public TopSortMerge(AttrType in1[], int len_in1, short s1_sizes[], AttrType in2[], int len_in2,
			short s2_sizes[],

			int join_col_in1, int sortFld1Len, int join_col_in2, int sortFld2Len,

			int amt_of_mem, Iterator am1, Iterator am2,

			boolean in1_sorted, boolean in2_sorted, TupleOrder order,

			CondExpr outFilter[], FldSpec proj_list[], int n_out_flds,int _num,int _innerCounter,int _outerCounter) throws JoinNewFailed,
			JoinLowMemory, SortException, TupleUtilsException, IOException

	{
		SortMerge smj = new SortMerge(in1, len_in1, s1_sizes, in2, len_in2, 
				s2_sizes, join_col_in1, sortFld1Len, join_col_in2, 
				sortFld2Len, amt_of_mem, am1, am2, 
				in1_sorted, in2_sorted, order, outFilter, 
				proj_list, n_out_flds, _innerCounter, _outerCounter);
		
		_in1 = new AttrType[in1.length];
		_in2 = new AttrType[in2.length];
		System.arraycopy(in1, 0, _in1, 0, in1.length);
		System.arraycopy(in2, 0, _in2, 0, in2.length);
		int count = 0;

		Jtuple = new Tuple();
		// CHANGED BY US....n_out_flds decreased by 1 in order to merge the two
		// score columns
		Jtypes = new AttrType[n_out_flds - 1];
		// AttrType[] Jtypes = new AttrType[n_out_flds];
		short[] ts_size = null;
		nOutFlds = n_out_flds - 1; // changed by us
		try {
			ts_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, in2, len_in2,
					s1_sizes, s2_sizes, proj_list, n_out_flds - 1);
			System.out.println(ts_size[1]);
		} catch (Exception e) {
			throw new TupleUtilsException(e, "Exception is caught by SortMerge.java");
		}


		// Now, that stuff is setup, all we have to do is a get_next !!!!
		 TupleOrder descending = new TupleOrder(TupleOrder.Descending);
		    Sort sort_names = null;
		    try {
		      sort_names = new Sort (Jtypes,(short)nOutFlds, ts_size,
					     (iterator.Iterator) smj, 3, descending, ts_size[0], 10);
		    }
		    catch (Exception e) {
		      System.err.println ("*** Error preparing for nested_loop_join");
		      System.err.println (""+e);
		      Runtime.getRuntime().exit(1);
		    }
		    
		    Tuple t = null;
		    sortTupleList = new ArrayList<Tuple>();
		    try {
		      while ((t = sort_names.get_next()) != null  && _num > count) {
		    	//sortTupleList.add(t);  
		        t.print(Jtypes);
		        count++;
		        //qcheck2.Check(t);
		      }
		    } catch (Exception e) {
		        System.err.println (""+e);
		        e.printStackTrace();
		        Runtime.getRuntime().exit(1);
		      }
	}
	
	public ArrayList<Tuple> getTopSortMergeTuple()
	{
		return sortTupleList;
	}
	
	public void printTopSortMergeTuple(){
		java.util.Iterator<Tuple> it = sortTupleList.iterator();
		while(it.hasNext()){
			Tuple t = it.next();
			try {
				t.print(Jtypes);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	

	
	

}
