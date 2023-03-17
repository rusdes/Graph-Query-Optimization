package operators.datastructures.kdtree;

/**
 * 
 * based on work by Simon Levy
 * http://www.cs.wlu.edu/~levy/software/kd/
 */

import java.io.Serializable;
import java.util.Vector;


/**
 * KDTree is a class supporting KD-tree insertion, deletion, equality search,
 * range search, and nearest neighbor(s) using double-precision floating-point
 * keys. Splitting dimension is chosen naively, by depth modulo K. Semantics are
 * as follows:
 * 
 * <UL>
 * <LI>Two different keys containing identical numbers should retrieve the same
 * value from a given KD-tree. Therefore keys are cloned when a node is
 * inserted. <BR>
 * <BR>
 * <LI>As with Hashtables, values inserted into a KD-tree are not cloned.
 * Modifying a value between insertion and retrieval will therefore modify the
 * value stored in the tree.
 *</UL>
 * 
 * @author Simon Levy, Bjoern Heckel
 * @version %I%, %G%
 * @since JDK1.2
 */
public class KDTree implements Serializable{

	// K = number of dimensions
	private int m_K;

	// root of KD-tree
	private KDNode m_root;

	/**
	 * Creates a KD-tree with specified number of dimensions.
	 * 
	 * @param k
	 *            number of dimensions
	 */
	public KDTree(int k) {

		m_K = k;
		m_root = null;
	}

	public Object getRoot(){
		return this.m_root.v;
	}

	/**
	 * Insert a node in a KD-tree. Uses algorithm translated from 352.ins.c of
	 * 
	 * <PRE>
	 *   &#064;Book{GonnetBaezaYates1991,                                   
	 *     author =    {G.H. Gonnet and R. Baeza-Yates},
	 *     title =     {Handbook of Algorithms and Data Structures},
	 *     publisher = {Addison-Wesley},
	 *     year =      {1991}
	 *   }
	 * </PRE>
	 * 
	 * @param key
	 *            key for KD-tree node
	 * @param value
	 *            value at that key
	 * 
	 * @throws KeySizeException
	 *             if key.length mismatches K
	 * @throws KeyDuplicateException
	 *             if key already in tree
	 */
	public void insert(String[] key, Object value) {

		if (key.length != m_K) {
			throw new RuntimeException("KDTree: wrong key size!");
		}

		else
			m_root = KDNode.ins(new HPoint(key), value, m_root, 0, m_K);
	}

	/**
	 * Find KD-tree node whose key is identical to key. Uses algorithm
	 * translated from 352.srch.c of Gonnet & Baeza-Yates.
	 * 
	 * @param key
	 *            key for KD-tree node
	 * 
	 * @return object at key, or null if not found
	 * 
	 * @throws KeySizeException
	 *             if key.length mismatches K
	 */
	public Object search(String[] key) {

		if (key.length != m_K) {
			throw new RuntimeException("KDTree: wrong key size!");
		}

		KDNode kd = KDNode.srch(new HPoint(key), m_root, m_K);

		return (kd == null ? null : kd.v);
	}

	/**
	 * Delete a node from a KD-tree. Instead of actually deleting node and
	 * rebuilding tree, marks node as deleted. Hence, it is up to the caller to
	 * rebuild the tree as needed for efficiency.
	 * 
	 * @param key
	 *            key for KD-tree node
	 * 
	 * @throws KeySizeException
	 *             if key.length mismatches K
	 * @throws KeyMissingException
	 *             if no node in tree has key
	 */
	public void delete(String[] key) {

		if (key.length != m_K) {
			throw new RuntimeException("KDTree: wrong key size!");
		}

		else {

			KDNode t = KDNode.srch(new HPoint(key), m_root, m_K);
			if (t == null) {
				throw new RuntimeException("KDTree: key missing!");
			} else {
				t.deleted = true;
			}

			// m_count--;
		}
	}

	/**
	 * Range search in a KD-tree. Uses algorithm translated from 352.range.c of
	 * Gonnet & Baeza-Yates.
	 * 
	 * @param lowk
	 *            lower-bounds for key
	 * @param uppk
	 *            upper-bounds for key
	 * 
	 * @return array of Objects whose keys fall in range [lowk,uppk]
	 * 
	 * @throws KeySizeException
	 *             on mismatch among lowk.length, uppk.length, or K
	 */
	public Object[] range(String[] lowk, String[] uppk) {

		if (lowk.length != uppk.length) {
			throw new RuntimeException("KDTree: wrong key size!");
		}

		else if (lowk.length != m_K) {
			throw new RuntimeException("KDTree: wrong key size!");
		}

		else {
			Vector<KDNode> v = new Vector<KDNode>();
			KDNode.rsearch(new HPoint(lowk), new HPoint(uppk), m_root, 0, m_K, v);
			Object[] o = new Object[v.size()];
			for (int i = 0; i < v.size(); ++i) {
				KDNode n = v.elementAt(i);
				o[i] = n.v;
			}
			return o;
		}
	}

	public String toString() {
		return m_root.toString(0);
	}
}
