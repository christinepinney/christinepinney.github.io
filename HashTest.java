import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Random;
import java.util.Scanner;

public class HashTest<H> extends HashTable<H> {
	
	public HashTest() {
		
	}

	public static void main(String[] args) {
		
		String str0 = args[0];
		String dtype;
		
		if (Integer.parseInt(str0) == 1) {
			
			dtype = "java.util.Random";
			
			HashTable<Integer> h1 = new HashTable<Integer>(true);
			HashTable<Integer> h2 = new HashTable<Integer>(false);
			
			Random rand = new Random();
			int a = 0;
			int i = 0;
			
			String str = args[1];
			DecimalFormat df = new DecimalFormat("#.#");
			DecimalFormatSymbols symbols = new DecimalFormatSymbols();
			symbols.setDecimalSeparator('.');
			df.setDecimalFormatSymbols(symbols);
			
			while (h1.getLoadFactor() < Double.parseDouble(str)) {
				a = rand.nextInt();
				h1.hashInsert(a);
				h2.hashInsert(a);
				i++;
			}
			
			System.out.println("A good table size is found: " + h1.getSize());
			System.out.println("Data source type: " + dtype);
			System.out.println();
			System.out.println("Using Linear Hashing...");
			System.out.println("Input " + i + " elements, of which " + h1.getDuplicateCount() + " are duplicates");
			System.out.println("load factor = " + df.format(h1.getLoadFactor()) + ", Avg. no. of probes " + h1.avgProbes());
			System.out.println();
			System.out.println("Using Double Hashing...");
			System.out.println("Input " + i + " elements, of which " + h2.getDuplicateCount() + " are duplicates");
			System.out.println("load factor = " + df.format(h2.getLoadFactor()) + ", Avg. no. of probes " + h2.avgProbes());
			
			if (args.length == 3) {
				String str2 = args[2];
				if (Double.parseDouble(str2) == 1) {
					try { 
						h1.dump("linear-dump");
						h2.dump("double-dump");
					} catch (Exception e) {
						e.printStackTrace();
					}
					
				}
				
			}
			
		} else if (Integer.parseInt(str0) == 2) {
			
			dtype = "System.currentTimeMillis()";
			
			HashTable<Long> h1 = new HashTable<Long>(true);
			HashTable<Long> h2 = new HashTable<Long>(false);
			
			long a = System.currentTimeMillis();
			int i = 0;
			
			String str = args[1];
			DecimalFormat df = new DecimalFormat("#.#");
			DecimalFormatSymbols symbols = new DecimalFormatSymbols();
			symbols.setDecimalSeparator('.');
			df.setDecimalFormatSymbols(symbols);
			
			while (h1.getLoadFactor() < Double.parseDouble(str)) {
				a = System.currentTimeMillis();
				h1.hashInsert(a);
				h2.hashInsert(a);
				i++;
			}
			
			System.out.println("A good table size is found: " + h1.getSize());
			System.out.println("Data source type: " + dtype);
			System.out.println();
			System.out.println("Using Linear Hashing...");
			System.out.println("Input " + i + " elements, of which " + h1.getDuplicateCount() + " are duplicates");
			System.out.println("load factor = " + df.format(h1.getLoadFactor()) + ", Avg. no. of probes " + h1.avgProbes());
			System.out.println();
			System.out.println("Using Double Hashing...");
			System.out.println("Input " + i + " elements, of which " + h2.getDuplicateCount() + " are duplicates");
			System.out.println("load factor = " + df.format(h2.getLoadFactor()) + ", Avg. no. of probes " + h2.avgProbes());
			
			if (args.length == 3) {
				String str2 = args[2];
				if (Double.parseDouble(str2) == 1) {
					try { 
						h1.dump("linear-dump");
						h2.dump("double-dump");
					} catch (Exception e) {
						e.printStackTrace();
					}
					
				}
				
			}
			
		} else if (Integer.parseInt(str0) == 3) {
		
			dtype = "word-list";
			
			HashTable<String> h1 = new HashTable<String>(true);
			HashTable<String> h2 = new HashTable<String>(false);
			
			try {
				Scanner scan = new Scanner(new File("word-list"));
				
				int i = 0;
				
				String str = args[1];
				DecimalFormat df = new DecimalFormat("#.#");
				DecimalFormatSymbols symbols = new DecimalFormatSymbols();
				symbols.setDecimalSeparator('.');
				df.setDecimalFormatSymbols(symbols);
				
				while (scan.hasNextLine() && (h1.getLoadFactor() < Double.parseDouble(str))) {
					
					String word = scan.nextLine();
					h1.hashInsert(word);
					h2.hashInsert(word);
					i++;
					
				}
				
				System.out.println("A good table size is found: " + h1.getSize());
				System.out.println("Data source type: " + dtype);
				System.out.println();
				System.out.println("Using Linear Hashing...");
				System.out.println("Input " + i + " elements, of which " + h1.getDuplicateCount() + " are duplicates");
				System.out.println("load factor = " + df.format(h1.getLoadFactor()) + ", Avg. no. of probes " + h1.avgProbes());
				System.out.println();
				System.out.println("Using Double Hashing...");
				System.out.println("Input " + i + " elements, of which " + h2.getDuplicateCount() + " are duplicates");
				System.out.println("load factor = " + df.format(h2.getLoadFactor()) + ", Avg. no. of probes " + h2.avgProbes());
			
				if (args.length == 3) {
					String str2 = args[2];
					if (Double.parseDouble(str2) == 1) {
						try { 
							h1.dump("linear-dump");
							h2.dump("double-dump");
						} catch (Exception e) {
							e.printStackTrace();
						}
						
					}
					
				}
				
			} catch (FileNotFoundException e) {
			
				e.printStackTrace();
			}
			
		}
		
	}

}
