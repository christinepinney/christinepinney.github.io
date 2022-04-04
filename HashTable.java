import java.io.FileNotFoundException;
import java.io.PrintStream;

public class HashTable<H> extends HashObject<H> {
	
	
	private HashObject<H>[] table;
	private int size;
	private int numEntries;
	private double avgProbes;
	private double loadFactor;
	private boolean linear;
	
	
	/* Default constructor */
	public HashTable() {
		
	}
	
	// Parameterized Constructor
	@SuppressWarnings("unchecked")
	public HashTable(boolean linear) {
		
		size = getPrimes();
		table = new HashObject[size];
		this.linear = linear;
		
	}
	
	// Hash functions
	private int hash(H object, int i) {
		
		int primaryHash;
		int secondaryHash;
		
		primaryHash = PositiveMod(object.hashCode(), size);
		
		if (!linear) {
			secondaryHash = 1 + PositiveMod(object.hashCode(), size - 2);
			return (primaryHash + (i * secondaryHash)) % size; 
		} else {
			return (primaryHash + i) % size;
		}
	
	}
	
	// Insert HashObject into HashTable
	public int hashInsert(H object) {
		
		int i = 0;
		while (i != size) {
			int j = hash(object, i);
			if (table[j] == null) {
				table[j] = new HashObject<H>(i + 1, object);
				numEntries++;
				return j;
			} else if (table[j].equals(object)) {
				table[j].increaseDuplicates();
				return j;
			}
			i++;
		}
		return -1;
	}

	// Get twin primes
	public int getPrimes() {
		
		int i = 0;
		
		for (i = 95501; i < 96000; i+=2) {
			if (isPrime(i) && isPrime(i + 2)) {
				return i + 2;
			}
			
		}
		return -1;	
	}
	
	// Check if number is prime
	public boolean isPrime(int prime) {
		
		int i = 0;
		
		for (i = 2; i < prime; i++) {
			if (prime % i == 0) {
				return false;
			} 
			
		}
		return true;
	}
	
	// Calculate load factor
	public double getLoadFactor() {
		
		loadFactor = (double)numEntries / size;
		
		return loadFactor;
	}
	
	// Get number of probes
	public int getDuplicateCount() {
				
		int totalDuplicates = 0;
		int i = 0;
		for (i = 0; i < size; i++) {
			if (table[i] != null) {
				totalDuplicates += table[i].getDuplicates();
			}
		}
		
		return totalDuplicates;
	}
	
	// Get average number of probes
	public double avgProbes() {
		
		int totalProbes = 0;
		int i = 0;
		for (i = 0; i < size; i++) {
			if (table[i] != null) {
				totalProbes += table[i].getProbes();
			}
		}
		
		avgProbes = (double)totalProbes / numEntries;
		
		return avgProbes;
	}
	
	// Handle negative hash values
	public int PositiveMod (int dividend, int divisor) {
		
		int value = dividend % divisor;
		if (value < 0) {
			value += divisor;
		}
		
		return value;
	}
	
	public void dump(String filename) throws FileNotFoundException {
		
		PrintStream ps = new PrintStream(filename);
		PrintStream stdout = System.out;
		ps.append(toString());
		System.setOut(ps);
		System.setOut(stdout);
	
		
	}
	
	// Get size 
	public int getSize() {
		
		return size;
	}
	
	@Override
	public String toString() {
		
		StringBuilder str = new StringBuilder();
		int i = 0;
		for (i = 0; i < size; i++) {
			if (table[i] != null) {
				str.append("table[");
				str.append(i);
				str.append("]: ");
				str.append(table[i].toString());
				str.append("\n");
			}
		}
		
		return str.toString();
	}
	
}