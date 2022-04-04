
public class HashObject<H> {
	
	public int duplicates;
	public int probeCount;
	private H object;
	
	/* Default Constructor */
	public HashObject() {
		
	}
	
	/* Parameterized Constructor 
	 * 
	 * @param int probeCount, H object
	 * 
	 */
	public HashObject(int probeCount, H object) {
		
		duplicates = 0;
		this.probeCount = probeCount;
		this.object = object;
	}
	
	// Get number of probes
	public int getProbes() {
		
		return probeCount;
	}
	
	// Get number of probes
	public int getDuplicates() {
			
		return duplicates;
	}
	
	// Increment duplicate count
	public void increaseDuplicates() {
		
		duplicates++;
	}
	
	// Compare objects
	@Override
	public boolean equals(Object o) {
		
		if (o.equals(object)) {
			return true;
		}
		
		return false;
	}
	
	@Override
	public String toString() {
		
		StringBuilder str = new StringBuilder();
		str.append(object);
		str.append(" ");
		str.append(duplicates);
		str.append(" ");
		str.append(probeCount);
		
		return str.toString();
	}
	
}
