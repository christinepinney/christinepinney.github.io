
/**
 * Cache implementation of double-linked list data structure that stores generic objects and uses MRU (Most Recently Used) scheme. 
 * 
 * @author cpinney
 *
 * @param <T>
 */

public class Cache<T> extends IUDoubleLinkedList<T> {
	
	int cacheSize;
	
	/** Create new cache */
	public Cache(int cacheSize) {
		super();
		this.cacheSize = cacheSize;
	}
	
	/** Add object to top of cache if cache has not reached its size limit 
	 * @param T object, object to be added
	 */
	public void addObject(T object) {
		if (super.size() == cacheSize) {
			super.removeLast();
		}
		super.addToFront(object);
	}
	
	/** Remove object from cache 
	 * @param T object, object to be removed
	 */
	public T removeObject(T object) {
		return super.remove(object);
	}
	
	/** Returns true if cache contains search object, else false 
	 * @param T object, object to search for in cache 
	 */
	public boolean getObject(T object) {
		return super.contains(object);
	}
	
	/** Empty the cache */
	public void clearCache() {
		while (!super.isEmpty()) {
			removeLast();
		}
	}

}
