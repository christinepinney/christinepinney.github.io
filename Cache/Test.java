import java.io.File;

import java.io.FileNotFoundException;
import java.util.InputMismatchException;
import java.util.Scanner;
import java.util.StringTokenizer;


public class Test {

	private static int cacheOneSize;
	private static int cacheTwoSize;
	private Cache<Object> cacheOne;
	private Cache<Object> cacheTwo;
	private int cacheOneHits;
	private int cacheOneRefs;
	private int cacheTwoHits;
	private int cacheTwoRefs;
	private String lineSearch;
	private String search;

	
	/** Usage message */
	private static void printUsage() {
		
		System.out.println("Usage:\n\tjava Test 1 <cache size> <input textfile name>\nOR\t" 
				+ "java Test 2 <1st-level cache size> <2nd-level cache size> <input textfile name>");
	}
	
	public static void main(String[] args) {
		
		// measure elapsed time to run program
		// long runningTime = System.currentTimeMillis();
		if (args[0].equals("1") && args.length == 3) {
			cacheOneSize = Integer.parseInt(args[1]);
			try {
				new Test(cacheOneSize, args[2]);
			} catch (FileNotFoundException e) {
				System.out.println();
				System.out.println("File not found.");
				System.out.println();
				printUsage();
				System.out.println();
			} catch (InvalidCommandLineArgsException e) {
				System.out.println();
				System.out.println("Invalid cache sizes. Second cache must be larger than first cache.");
				System.out.println();
				printUsage();
				System.out.println();
			}
		} else if (args[0].equals("2") && args.length == 4) {
			cacheOneSize = Integer.parseInt(args[1]);
			cacheTwoSize = Integer.parseInt(args[2]);
			try {
				new Test(cacheOneSize, cacheTwoSize, args[3]);
			} catch (FileNotFoundException e) {
				System.out.println();
				System.out.println("File not found.");
				System.out.println();
				printUsage();
				System.out.println();
			} catch (InvalidCommandLineArgsException e) {
				System.out.println();
				System.out.println("Invalid cache sizes. Second cache must be larger than first cache.");
				System.out.println();
				printUsage();
				System.out.println();
			}
		} else {
			System.out.println();
			System.out.println("Invalid command line arguments.");
			System.out.println();
			printUsage();
			System.out.println();
		}
		
		// System.out.println(System.currentTimeMillis() - runningTime);
	}
	
	
	
	public Test(int firstCacheSize, String filename) throws FileNotFoundException {
		
		cacheOne = new Cache<Object>(firstCacheSize);
		
		Scanner fileScan = new Scanner(new File(filename));
		
		System.out.print("\nCache with " + firstCacheSize + " entries has been created" + "\n");
		
		try {
			while (fileScan.hasNextLine()) {
				// scan file
				lineSearch = fileScan.nextLine();
				StringTokenizer st = new StringTokenizer(lineSearch, "\t ");
				while (st.hasMoreTokens()) {
					search = st.nextToken();
					// check to see if cache is empty before searching it
					if (!cacheOne.isEmpty()) {
						if (cacheOneRefs % 46000 == 0) {
							System.out.print(".");
						}
						cacheOneRefs++;
						// if cache isn't empty, check to see if any entries match the current search object
						if (cacheOne.getObject(search)) {
							// if cache one hits, move the object to the top of cache
							cacheOneHits++; // increment cache hits
							cacheOne.removeObject(search);
							cacheOne.addObject(search);
							// if cache one misses, add search object
						} else {
							cacheOne.addObject(search);
						}
						// if cache is empty, add search object
					} else {
						cacheOne.addObject(search);
					}

				}
			}
			System.out.println();
		} catch (InputMismatchException e) {
			e.toString();
		}
		// calculate ratio
		double firstRatio = (double)cacheOneHits / cacheOneRefs;
		
		// print results 
		System.out.print("The number of cache references: " + cacheOneRefs + "\n" +
				"The number of cache hits: " + cacheOneHits + "\n" +
				"The cache hit ratio" + "\t" + "\t" + "   : " + firstRatio + "\n" + "\n" +
				"----------------------------------------------------------------" + "\n");
		fileScan.close();
	}
	
	public Test(int firstCacheSize, int secondCacheSize, String filename) throws FileNotFoundException {
	
		cacheOne = new Cache<Object>(firstCacheSize);
		cacheTwo = new Cache<Object>(secondCacheSize);

		// make sure first cache is smaller than second cache
		if (cacheOneSize >= cacheTwoSize) {
			throw new InvalidCommandLineArgsException("");
		}
		
		Scanner fileScan = new Scanner(new File(filename));
		
		System.out.print("\nFirst level cache with " + firstCacheSize + " entries has been created" + "\n" +
				"Second level cache with " + secondCacheSize + " entries has been created" + "\n");
		
		try {
			while (fileScan.hasNextLine()) {
				// scan file
				lineSearch = fileScan.nextLine();
				StringTokenizer st = new StringTokenizer(lineSearch, "\t ");
				while (st.hasMoreTokens()) {
					search = st.nextToken();
					// check to see if cache is empty before searching it
					if (!cacheOne.isEmpty()) {
						if (cacheOneRefs % 46000 == 0) {
							System.out.print(".");
						}
						cacheOneRefs++;
						// if cache isn't empty, check to see if any entries match the current search object
						if (cacheOne.getObject(search)) {
							// if cache one hits, move the object to the top of both cache
							cacheOneHits++; // increment cache one hits
							cacheOne.removeObject(search);
							cacheOne.addObject(search);
							cacheTwo.removeObject(search);
							cacheTwo.addObject(search);

						} else {
							// if we don't find the object in cache one, search cache two
							if (cacheTwo.getObject(search)) {
								// if cache two hits, move object to top of cache two and add object to top of cache one
								cacheTwoHits++; // increment cache two hits
								cacheTwo.removeObject(search);
								cacheTwo.addObject(search);
								cacheOne.addObject(search);
							// if we don't find the object in cache two, add object to top of both cache
							} else {
								cacheOne.addObject(search);
								cacheTwo.addObject(search);
							}
						
						}
					// if cache is empty, add the search object to top of both cache
					} else {
						cacheOne.add(search);
						cacheTwo.add(search);
						cacheOneRefs++;
					} 
					
				}
				
			}
			System.out.println();
		} catch (InputMismatchException e) {
			e.toString();
		}
		// calculate ratios
		cacheTwoRefs = cacheOneRefs - cacheOneHits;
		int globalHits = cacheOneHits + cacheTwoHits;
		double globalRatio = (double)globalHits / cacheOneRefs;
		double firstRatio = (double)cacheOneHits / cacheOneRefs;
		double secondRatio = (double)cacheTwoHits / cacheTwoRefs;
		
		// print results
		System.out.print("The number of global references: " + cacheOneRefs + "\n" +
				"The number of global cache hits: " + globalHits + "\n" + 
				"The global hit ratio" + "\t" + "\t" + "      : " + globalRatio + "\n" + "\n" +
				"The number of 1st-level references: " + cacheOneRefs + "\n" +
				"The number of 1st-level cache hits: " + cacheOneHits + "\n" +
				"The 1st-level cache hit ratio" + "\t" + "\t" + "    : " + firstRatio + "\n" + "\n" +
				"The number of 2nd-level references: " + cacheTwoRefs + "\n" +
				"The number of 2nd-level cache hits: " + cacheTwoHits + "\n" +
				"The 2nd-level cache hit ratio" + "\t" + "\t" + "    : " + secondRatio + "\n" + "\n"
				+ "----------------------------------------------------------------" + "\n"
				);
		fileScan.close();
		
	}
		
}

