import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;
/**
 * Double-linked node implementation of IndexedUnsortedList. Private inner 
 * class DLLIterator implements ListIterator for IUDoubleLinkedList, which 
 * also serves as Iterator for IUDLL. Requires private class Node<T> for 
 * creating linked linear data structure.  
 * 
 * @author cpinney
 */
public class IUDoubleLinkedList<T> implements IndexedUnsortedList<T> {
	private Node<T> head;
	private Node<T> tail;
	private int size;
	private int version;
	
	/** Initialize a new empty list */
	public IUDoubleLinkedList() {
		head = tail = null;
		size = 0;
		version = 0;
	}

	@Override
	public void addToFront(T element) {
		Node<T> newNode = new Node<T>(element);
		newNode.setNextNode(head);
		if (isEmpty()) {
			tail = newNode;
		} else {
			head.setPrevNode(newNode);
		}
		head = newNode;
		size++;
		version++;
	}

	@Override
	public void addToRear(T element) {
		Node<T> newNode = new Node<T>(element);
		newNode.setPrevNode(tail);
		if (isEmpty()) {
			head = newNode;
		} else {
			tail.setNextNode(newNode);
		}
		tail = newNode;
		size++;
		version++;
	}

	@Override
	public void add(T element) {
		addToRear(element);
	}

	@Override
	public void addAfter(T element, T target) {
		Node<T> targetNode = head;
		while (targetNode != null && !targetNode.getElement().equals(target)) {
			targetNode = targetNode.getNextNode();
		}
		if (targetNode == null) {
			throw new NoSuchElementException();
		}
		Node<T> newNode = new Node<T>(element);
		newNode.setNextNode(targetNode.getNextNode());
		newNode.setPrevNode(targetNode);
		if (targetNode == tail) {
			tail = newNode;
		} else {
			targetNode.getNextNode().setPrevNode(newNode);
		}
		targetNode.setNextNode(newNode);
		size++;
		version++;
	}

	@Override
	public void add(int index, T element) {
		if (index < 0 || index > size) {
			throw new IndexOutOfBoundsException();									        
		}
		Node<T> newNode = new Node<T>(element);
		if (isEmpty()) {
			newNode = head = tail;
		} else {
			int currIndex = 0;
			Node<T> current = head;
			while (currIndex < index-1) {
				current = current.getNextNode();
				currIndex++;
			}
			if (current == null) {
				tail.setNextNode(newNode);
				tail = newNode;
			} else if (index == 0) {
				addToFront(element);
				size--;
				version--;
			} else if (size == 1 && index == 1) {
				addToRear(element);
				size--;
				version--;
			} else if (current == tail) {
				addToRear(element);
				size--;
				version--;
			} else {
				newNode.setNextNode(current.getNextNode());
				current.setNextNode(newNode);
				newNode.setPrevNode(current);
			}
		}
		size++;
		version++;
	}

	@Override
	public T removeFirst() {
		if (isEmpty()) {
			throw new NoSuchElementException();
		}
		T retVal = head.getElement();
		head = head.getNextNode();
		if (head == null) {
			tail = null;
		}
		size--;
		version++;
		return retVal;
	}

	@Override
	public T removeLast() {
		if (isEmpty()) {
			throw new NoSuchElementException();
		}
		T retVal = tail.getElement();
		if (tail == head) {
			head = tail = null;
		} else {
			tail.getPrevNode().setNextNode(null);
			tail = tail.getPrevNode();
		}
		size--;
		version--;
		return retVal;
	}

	@Override
	public T remove(T element) {
		if (isEmpty()) {
			throw new NoSuchElementException();
		}
		boolean found = false;
		Node<T> current = head;
		while (current != null && !found) {
			if (element.equals(current.getElement())) {
				found = true;
			} else {
				current = current.getNextNode();
			}
		}
		if (!found) {
			throw new NoSuchElementException();
		}
		T retVal = current.getElement();
		if (size == 1) {
			head = tail = null;
		} else if (current == head) {
			head = current.getNextNode();
			head.setPrevNode(null);
		} else if (current == tail) {
			tail = current.getPrevNode();
			tail.setNextNode(null);
		} else {
			current.getNextNode().setPrevNode(current.getPrevNode());
			current.getPrevNode().setNextNode(current.getNextNode());
		}
		size--;
		version++;
		
		return retVal;
	}

	@Override
	public T remove(int index) {
		if (index < 0 || index >= size) {
			throw new IndexOutOfBoundsException();        								
		}
		if (isEmpty()) {
			throw new NoSuchElementException();
		}
		Node<T> targetNode = head;															
		int currIndex = 0;
		while (currIndex < index) {
			targetNode = targetNode.getNextNode();
			currIndex++;
		} 
		// single element
		if (size == 1) {
			head = tail = null;
		// removing head
		} else if (index == 0) {
			head = targetNode.getNextNode();
			targetNode.getNextNode().setPrevNode(null);
		// removing the tail
		} else if (targetNode == tail) {
			tail = targetNode.getPrevNode();
			targetNode.getPrevNode().setNextNode(null);
		// generalized case somewhere in middle of list
		} else {
			targetNode.getPrevNode().setNextNode(targetNode.getNextNode());
			targetNode.getNextNode().setPrevNode(targetNode.getPrevNode());
		}
		size--;
		version++;
		return targetNode.getElement();
	}

	@Override
	public void set(int index, T element) {
		if (index < 0 || index >= size) {
			throw new IndexOutOfBoundsException();         									
		}
		if (isEmpty()) {
			throw new NoSuchElementException();
		}
		Node<T> current = head;
		int currIndex = 0;
		while (currIndex < index) {
			current = current.getNextNode();
			currIndex++;
		}
		if (size == 1) {
			head.setElement(element);
			tail.setElement(element);
		} else if (current == head) {
			head.setElement(element);
		} else if (current == tail) {
			tail.setElement(element);
		} else {
			current.setElement(element);
		}
		version++;
	}

	@Override
	public T get(int index) {
		if (index < 0 || index >= size) {
			throw new IndexOutOfBoundsException();
		}
		int currIndex = 0;
		Node<T> current = head;
		while (currIndex < index) {
			current = current.getNextNode();
			currIndex++;
		}
		return current.getElement();
	}

	@Override
	public int indexOf(T element) {
		// start at the beginning
		Node<T> current = head; 
		int currIndex = 0;
		// until we run out of nodes or find the node we are looking for
		while (current != null && !element.equals(current.getElement())) { 
			current = current.getNextNode();
			currIndex++;
		}
		if (current == null) {
			currIndex = -1;
		}
		return currIndex;
	}

	@Override
	public T first() {
		if (isEmpty()) {
			throw new NoSuchElementException();
		}
		return head.getElement();
	}

	@Override
	public T last() {
		if (isEmpty()) {
			throw new NoSuchElementException();
		}
		return tail.getElement();
	}

	@Override
	public boolean contains(T target) {
		return indexOf(target) > -1;
	}

	@Override
	public boolean isEmpty() {
		return head == null;
	}

	@Override
	public int size() {
		return size;
	}
	
	@Override 
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append("[");
		for (T item : this) {
			str.append(item.toString());
			str.append(", ");
		}
		
		if (!isEmpty()) {
			str.delete(str.length()-2, str.length());
		}
		str.append("]");
		return str.toString();
	}

	@Override
	public Iterator<T> iterator() {
		return new DLLIterator();
	}

	@Override
	public ListIterator<T> listIterator() {
		return new DLLIterator();
	}

	@Override
	public ListIterator<T> listIterator(int startingIndex) {
		return new DLLIterator(startingIndex);
	}
	
	/** ListIterator for IUDoubleLinkedList - also serves as Iterator for IUDLL */
	private class DLLIterator implements ListIterator<T> {
		private Node<T> nextNode;
		private int nextIndex;
		private Node<T> lastReturned;
		private int iterVersion;
		
		/** Initialize new DLLIterator in front of first element */
		private DLLIterator() {
			this(0);
		}
		
		/** Initialize new DLLIterator in front of given index 
		 * @param startingIndex index of element that would be next.
		 */
		private DLLIterator(int startingIndex) {
			if (startingIndex < 0 || startingIndex > size) {
				throw new IndexOutOfBoundsException();
			}
			nextNode = head;
			
			// TODO: insert optimization from n/2 to n/4: check where we are and navigate from the right end!!
			
			for (int i = 0; i < startingIndex; i++) {
				nextNode = nextNode.getNextNode();
			}
			nextIndex = startingIndex;
			lastReturned = null;
			iterVersion = version;
		}
		
		@Override
		public boolean hasNext() {
			if (iterVersion != version) {
				throw new ConcurrentModificationException();
			}
			return nextNode != null;
		}

		@Override
		public T next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			T retVal = nextNode.getElement();
			// timing is important here
			lastReturned = nextNode;
			nextNode = nextNode.getNextNode();
			nextIndex++;
			return retVal;
		}

		@Override
		public boolean hasPrevious() {
			if (iterVersion != version) {
				throw new ConcurrentModificationException();
			}
			return nextNode != head;
		}

		@Override
		public T previous() {
			if (!hasPrevious()) {
				throw new NoSuchElementException();
			}
			if (nextNode == null) {
				nextNode = tail;
			} else {
				nextNode = nextNode.getPrevNode();
			}
			// timing is important here
			lastReturned = nextNode;
			nextIndex--;
			return nextNode.getElement();
		}

		@Override
		public int nextIndex() {
			return nextIndex;
		}

		@Override
		public int previousIndex() {
			return nextIndex - 1;
		}

		@Override
		public void remove() {
			if (iterVersion != version) {
				throw new ConcurrentModificationException();
			}
			if (lastReturned == null) {
				throw new IllegalStateException();
			}
			if (lastReturned == head) {
				head = head.getNextNode();
			} else {
				lastReturned.getPrevNode().setNextNode(lastReturned.getNextNode());
			}
			if (lastReturned == tail) {
				tail = tail.getPrevNode();
			} else {
				lastReturned.getNextNode().setPrevNode(lastReturned.getPrevNode());
			}
			// last move was previous
			if (nextNode == lastReturned) {
				nextNode = nextNode.getNextNode();
			// last move was next
			} else {
				nextIndex--;
			}
			size--;
			version++;
			iterVersion++;
			lastReturned = null;
		}

		@Override
		public void set(T e) {
			if (iterVersion != version) {
				throw new ConcurrentModificationException();
			}
			if (lastReturned == null) {
				throw new IllegalStateException();
			}
			if(size == 1) {
				head.setElement(e);
				tail.setElement(e);
			}
			if (lastReturned == head) {
				head.setElement(e);
			}
			if (lastReturned == tail) {
				tail.setElement(e);
			} else {
				lastReturned.setElement(e);
			}
			version++;
			iterVersion++;
			lastReturned = null;
		}

		@Override
		public void add(T e) {
			if (iterVersion != version) {
				throw new ConcurrentModificationException();
			}
			Node<T> newNode = new Node<T>(e);
			if (nextNode != null) {
				newNode.setNextNode(nextNode);
				newNode.setPrevNode(nextNode.getPrevNode());
				nextNode.setPrevNode(newNode);
			// adding at the end of a list 
			} else {
				newNode.setPrevNode(tail);
				tail = newNode;
			}
			// adding at the beginning of a list
			if (nextNode != head) {
				newNode.getPrevNode().setNextNode(newNode);
			} else {
				head = newNode;
			}
			nextIndex++;
			size++;
			version++;
			iterVersion++;
			lastReturned = null;
		}
		
	}

}
