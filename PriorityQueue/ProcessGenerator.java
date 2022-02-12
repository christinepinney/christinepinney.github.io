import java.util.Random;

/* 
 * Generates processes for priority queue
 * 
 * @author cpinney
 */

public class ProcessGenerator {

	private double prob1;
	Random rand = new Random(1);
	
	public ProcessGenerator(double probability) {
		
		this.prob1 = probability;
//		int currentTime = rand.nextInt();
//		int maxProcessTime = rand.nextInt();
//		int maxLevel = rand.nextInt();
//		
//		getNewProcess(currentTime, maxProcessTime, maxLevel);
		
	}

	/* Returns new Process
	 * 
	 * @param current time of arrival, max processing time, max priority level
	 */
	public Process getNewProcess(int currentTime, int maxProcessTime, int maxLevel) {
		
		int priorityLevel = rand.nextInt(maxLevel) + 1;
		int requiredProcessingTime = rand.nextInt(maxProcessTime) + 1;
		
		return new Process(currentTime, requiredProcessingTime, priorityLevel);
	}
	
	/* Check to see if there is a new process arrival
	 * 
	 * Returns true if there is a new arrival
	 */
	public boolean query() {
		double prob2 = rand.nextDouble();
		
		if (prob2 < prob1) {
			return true;
		} else {
			return false;
		}
	}
	
	/* Get probability of process arrival */
	public double getProbability() {
		return prob1;
	}
	
}
