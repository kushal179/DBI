package iterator;

import java.util.*;

public class MinHeap extends Heap{
  List<HeapElement> h = new ArrayList<HeapElement>();

  public MinHeap() {
  }

  
  
  public MinHeap(HeapElement[] keys) {
    for (HeapElement key : keys) {
      h.add(key);
    }
    for (int k = h.size() / 2 - 1; k >= 0; k--) {
      percolateDown(k, h.get(k));
    }
  }

  public void add(HeapElement node) {
    h.add(null);
    int k = h.size() - 1;
    while (k > 0) {
      int parent = (k - 1) / 2;
      HeapElement p = h.get(parent);
      if (node.score.compareTo(p.score) >= 0) {
        break;
      }
      h.set(k, p);
      k = parent;
    }
    h.set(k, node);
  }

  public HeapElement remove() {
	  HeapElement removedNode = h.get(0);
	  HeapElement lastNode = h.remove(h.size() - 1);
    percolateDown(0, lastNode);
    return removedNode;
  }

  public HeapElement min() {
    return h.get(0);
  }

  public boolean isEmpty() {
    return h.isEmpty();
  }

  void percolateDown(int k, HeapElement node) {
    if (h.isEmpty()) {
      return;
    }
    while (k < h.size() / 2) {
      int child = 2 * k + 1;
      if (child < h.size() - 1 && h.get(child).score.compareTo(h.get(child + 1).score) > 0) {
        child++;
      }
      if (node.score.compareTo(h.get(child).score) <= 0) {
        break;
      }
      h.set(k, h.get(child));
      k = child;
    }
    h.set(k, node);
  }
  
  void clear()
  {
	  h.clear();
  }
  public int size()
  {
	 return h.size(); 
  }
  
  public static void main(String[] args) {
	    
	   
	/*  HeapElement type1= new HeapElement(2,1);
	  
	  HeapElement type2=new HeapElement(5,2);
	   
	  HeapElement type3=new HeapElement(1,3);
	 
	  HeapElement type4=new HeapElement(3,4);
	    myType[] l={type1,type2,type3,type4};*/
	    MinHeap heap=new MinHeap();
	    // print keys in sorted order
	    while (!heap.isEmpty()) {
	      System.out.println(heap.remove().heapElement);
	    }
	  }
}
