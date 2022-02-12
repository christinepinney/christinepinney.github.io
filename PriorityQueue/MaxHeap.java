import java.util.Arrays;

/* 
 * Array representation of MaxHeap binary tree structure 
 * 
 * @author cpinney
 */

public class MaxHeap {

	private final int DEFAULT_CAPACITY = 50;
	private Process[] H;
	private int size;
	
	
	
	/* Max Heap Constructor */
	public MaxHeap() {
		
		H = new Process[DEFAULT_CAPACITY];
		size = 0;
		
	}
	
	/* Returns index of parent node of a given node
	 * 
	 * @param node index
	 */
	private int parent(int i) {
		return (i / 2);
	}
	
	/* Returns index of left child node of a given node
	 * 
	 * @param node index
	 */
	private int leftChild(int i) {
		return (2 * i);
	}
	
	/* Returns index of right child node of a given node
	 * 
	 * @param node index
	 */
	private int rightChild(int i) {
		return ((2 * i) + 1);
	}
	
	/* Shift given node UP to maintain heap 
	 * 
	 * @param node index, process 
	 */
	public <H> void MaxHeapifyUp(int i, Process key) {
		
		H[i] = key;
		
		while (i > 1 && H[parent(i)].compareTo(H[i]) == -1) {
			
			// swap parent and current node
			swap(parent(i), i);
			i = parent(i);
			
		}
		
	}
	
	/* Shift given node DOWN to maintain heap 
	 * 
	 * @param node index
	 */
	public <H> void MaxHeapifyDown(int i) {
		
		int maxIndex = i;
		
		// left child
		int l = leftChild(i);
		
		if (l <= size && H[l].compareTo(H[i]) == 1) {
			maxIndex = l;
		} else {
			maxIndex = i;
		}
		
		// right child
		int r = rightChild(i);
		
		if (r <= size && H[r].compareTo(H[maxIndex]) == 1) {
			maxIndex = r;
		}
		
		// i not same as maxIndex
		if (i != maxIndex) {
			swap(i, maxIndex);
			MaxHeapifyDown(maxIndex);
		}
		
	}
	
	/* Insert given node into max heap 
	 * 
	 * @param Process p
	 */
	public void insert(Process p) {
		
		// if array is full
		expandCapacity();
		
		size++;
		H[size] = p;
		
		// max heapify up to maintain heap
		MaxHeapifyUp(size, p);
	}
	
	/* Extract the node with the highest priority
	 * 
	 * Returns highest priority node
	 */
	public Process extractMax() {
		
		if (size < 1) {
			return null;
		}
		
		Process maxIndex = H[1];
		
		H[1] = H[size];
		size--;
		MaxHeapifyDown(1);
		
		return maxIndex;
	}
	
	/* Update the priority level of given node 
	 * 
	 * @param time to increment priority level, max priority level, key
	 */
	public void update(int timeToIncrementLevel, int maxLevel, int i) {
		
		H[i].incrementTimeNotProcessed();
		
		if (H[i].getTimeNotProcessed() >= timeToIncrementLevel) {
			H[i].resetTimeNotProcessed();
			if (H[i].getPriority() < maxLevel) {
				H[i].incrementPriorityLevel();
				MaxHeapifyUp(i, H[i]);
			}
		}
		
	}
	
	/* Returns highest priority node */
	public Process getMax() {
		
		return H[1];
	}
	
	/* Swap nodes 
	 * 
	 * @param first node index and second node index
	 */
	private void swap(int i, int j) {
		
		Process temp = H[i];
		H[i] = H[j];
		H[j] = temp;
	}
	
	/* Increase array size if necessary */
	private void expandCapacity() {
		if (size == H.length-1) {
			H = Arrays.copyOf(H, H.length * 2);
		}
	}
	
	/* Returns true if heap is empty */
	public boolean isEmpty() {
		return size == 0;
	}
	
}
