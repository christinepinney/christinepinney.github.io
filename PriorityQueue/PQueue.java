
/* 
 * Priority Queue implementation of MaxHeap
 * 
 * @author cpinney
 */

public class PQueue {

	private int size;
	private MaxHeap maxHeap;
	
	
	/* Priority Queue constructor */
	public PQueue() {
		maxHeap = new MaxHeap();
		size = 0;
	}
	
	/* Add process to queue
	 * 
	 * @param Process to be added
	 */
	public void enPQueue(Process p) {
		
		maxHeap.insert(p);
		size++;
	}
	
	/* Extract process with max priority level 
	 * 
	 * Returns process with max priority level 
	 */
	public Process dePQueue() {
		size--;
		return maxHeap.extractMax();
		
	}
	
	/* Update all jobs in process after extracting max 
	 * 
	 * @param time to increment priority level, max priority level
	 */
	public void update(int timeToIncrementLevel, int maxLevel) {
		
		for (int i = 1; i <= size; i++) {
			maxHeap.update(timeToIncrementLevel, maxLevel, i); 
		}
		
	}
	
	/* Returns true if queue is empty */
	public boolean isEmpty() {
		return size == 0;
	}
	
	/* Returns size of queue */
	public int size() {
		return size;
	}
	
}
