****************
* Cache
* cs321
* 1/19/2022
* Christine Pinney
**************** 

OVERVIEW:

 Cache is a doubly-linked list implementation of the IUDoubleLinkedList class. 
 The IUDoubleLinkedList implements the IndexedUnsortedList interface, and 
 Cache extends the IUDoubleLinkedList.


INCLUDED FILES:

 * Cache.java - cache implementation of IUDoubleLinkedList
 * Test.java - driver class to test cache implementation
 * IUDoubleLinkedList.java - doubly-linked list implementation of IndexedUnsortedList
 * IndexedUnsortedList.java - implemented interface
 * Node.java - supports IUDLL's linked linear data structure
 * InvalidCommandLineArgsException.java - Exception class
 * README - this file


COMPILING AND RUNNING:

 The elapsed time (in milliseconds) to run the following program is 16,603 ms (average):
 * $ java Test 2 1000 2000 Encyclopedia.txt
 
 From the directory containing all source files, compile the
 driver class (and all dependencies) with the command:
 * $ javac *.java

 Run the compiled class file with the command:
 * $ java Test 1 <cache size> <input textfile name> for one-level cache OR
 * $ java Test 2 <1st-level cache size> <2nd-level cache size> <input textfile name>

 Console output will give the formatted results of 1st-level, 2nd-level and global cache 
 references, hits, and hit ratios. 

----------------------------------------------------------------------------