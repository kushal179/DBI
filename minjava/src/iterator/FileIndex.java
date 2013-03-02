package iterator;

import btree.BTreeFile;
import heap.Heapfile;
import heap.Tuple;

public class FileIndex {
	
	public Heapfile heapfile;
	public BTreeFile btindex;
	public FileIndex (Heapfile s, BTreeFile t)
	{
		this.heapfile = s;
		this.btindex = t;
	}

}
