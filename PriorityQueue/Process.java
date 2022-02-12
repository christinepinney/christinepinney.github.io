import java.util.Random;

/* 
 * Creates process from given parameters and the use of random.nextInt()
 * 
 * @author cpinney
 */

public class Process {

	private long arrivalTime;
	private int priorityLevel;
	private int requiredProcessingTime;
	private int timeNotProcessed;
	private int timeRemaining;
	
	
	/* Process Constructor */
	public Process(int currentTime, int maxProcessTime, int maxLevel) {
		
		this.arrivalTime = currentTime;
		this.timeNotProcessed = 0;
		this.requiredProcessingTime = maxProcessTime;
		this.priorityLevel = maxLevel;
		this.timeRemaining = requiredProcessingTime;
		
	}
	
	/* Compares processes based on priority level, then arrival time 
	 * 
	 * @param Process to be compared with
	 * 
	 * Returns integer value
	 */
	public int compareTo(Process p) {
		
		if (priorityLevel > p.priorityLevel) {
			return 1;
		} 
		
		if (priorityLevel < p.priorityLevel) {
			return -1;
		}
		
		if (arrivalTime < p.arrivalTime) {
			return 1;
		}
		
		return -1;
	}
	
	/* Get arrival time */
	public long getArrivalTime() {
		
		return arrivalTime;
	}
	
	/* Get time remaining */
	public long getTimeRemaining() {
		
		return timeRemaining;
	}
	
	/* Reduce time remaining */
	public void reduceTimeRemaining() {
		
		timeRemaining--;
		timeNotProcessed = 0;
	}
	
	/* Get priority level */
	public int getPriority() {
		
		return priorityLevel;
	}
	
	/* Increment priority level */
	public void incrementPriorityLevel() {
		
		priorityLevel++;
	}
	
	/* Get time not processed */
	public int getTimeNotProcessed() {
		
		return timeNotProcessed;
	}
	
	/* Reset time not processed */
	public void resetTimeNotProcessed() {
		
		timeNotProcessed = 0;
	}
	
	/* Increment time not processed */
	public void incrementTimeNotProcessed() {
		
		timeNotProcessed++;
	}
	
	/* Returns true if process is done */
	public boolean finish() {
		
		return timeRemaining == 0;
	}

	
	

}
