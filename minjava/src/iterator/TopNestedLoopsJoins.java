package iterator;

import global.AttrType;
import global.TupleOrder;
import heap.Tuple;

import java.io.IOException;

/**
 * 
 * This file contains an implementation of the nested loops join algorithm as
 * described in the Shapiro paper. The algorithm is extremely simple:
 * 
 * foreach tuple r in R do foreach tuple s in S do if (ri == sj) then add (r, s)
 * to the result.
 */

public class TopNestedLoopsJoins extends NestedLoopsJoins {
	private Tuple Jtuple; // Joined tuple
	private int nOutFlds;


	/**
	 * constructor Initialize the two relations which are joined, including
	 * relation type,
	 * 
	 * @param in1
	 *            Array containing field types of R.
	 * @param len_in1
	 *            # of columns in R.
	 * @param t1_str_sizes
	 *            shows the length of the string fields.
	 * @param in2
	 *            Array containing field types of S
	 * @param len_in2
	 *            # of columns in S
	 * @param t2_str_sizes
	 *            shows the length of the string fields.
	 * @param amt_of_mem
	 *            IN PAGES
	 * @param am1
	 *            access method for left i/p to join
	 * @param relationName
	 *            access hfapfile for right i/p to join
	 * @param outFilter
	 *            select expressions
	 * @param rightFilter
	 *            reference to filter applied on right i/p
	 * @param proj_list
	 *            shows what input fields go where in the output tuple
	 * @param n_out_flds
	 *            number of outer relation fileds
	 * @exception IOException
	 *                some I/O fault
	 * @exception NestedLoopException
	 *                exception from this class
	 */
	public TopNestedLoopsJoins(AttrType in1[], int len_in1, short t1_str_sizes[], AttrType in2[], int len_in2, short t2_str_sizes[], int amt_of_mem,
			Iterator am1, String relationName, CondExpr outFilter[], CondExpr rightFilter[], FldSpec proj_list[], int n_out_flds, int _num, int innerCounter,
			int outerCounter) throws IOException, NestedLoopException {
		NestedLoopsJoins nlj = new NestedLoopsJoins(in1, len_in1, t1_str_sizes, in2, len_in2, t2_str_sizes, amt_of_mem, am1, relationName, outFilter,
				rightFilter, proj_list, n_out_flds, innerCounter, outerCounter);


		AttrType[] Jtypes = new AttrType[n_out_flds];
		short[] t_size;
		Jtuple = new Tuple();
		nOutFlds = n_out_flds;
		int count = 0;
		try {
			t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, in2, len_in2, t1_str_sizes, t2_str_sizes, proj_list, n_out_flds);
		} catch (TupleUtilsException e) {
			throw new NestedLoopException(e, "TupleUtilsException is caught by NestedLoopsJoins.java");
		}

		TupleOrder descending = new TupleOrder(TupleOrder.Descending);
		Sort sort_names = null;
		try {
			sort_names = new Sort(Jtypes, (short) nOutFlds, t_size, (iterator.Iterator) nlj, Jtypes.length, descending, 30, amt_of_mem);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}

		Tuple t = null;
		try {
			while ((t = sort_names.get_next()) != null && _num > count) {
				t.print(Jtypes);
				count++;
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

	}

}
