
/**
 * Node for creating linked linear data structures
 * @author cpinney	
 */

public class Node<T> {
	
	private T element;
	private Node<T> nextNode;
	private Node<T> prevNode;
	
	/**
	 * Initialize a new Node
	 */
	public Node(T element) {
		this.element = element;
		nextNode = null;
		prevNode = null;
	}

	/**
	 * @return the element
	 */
	public T getElement() {
		return element;
	}

	/**
	 * @param element the element to set
	 */
	public void setElement(T element) {
		this.element = element;
	}

	/**
	 * @return the nextNode
	 */
	public Node<T> getNextNode() {
		return nextNode;
	}

	/**
	 * @param nextNode the nextNode to set
	 */
	public void setNextNode(Node<T> nextNode) {
		this.nextNode = nextNode;
	}

	/**
	 * @return the prevNode
	 */
	public Node<T> getPrevNode() {
		return prevNode;
	}

	/**
	 * @param prevNode the prevNode to set
	 */
	public void setPrevNode(Node<T> prevNode) {
		this.prevNode = prevNode;
	}
	
	

}
