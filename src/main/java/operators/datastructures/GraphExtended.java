package operators.datastructures;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeSet;

import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Triplet;

import me.tongfei.progressbar.ProgressBar;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import operators.datastructures.kdtree.KDTree;
import operators.datastructures.kdtree.QuickSelect;

/**
 * Extended graph for Cypher Implementation
 * 
 * @param <K>  the key type for vertex identifiers
 * @param <VL> the value type of vertex labels
 * @param <VP> the value type of vertex properties
 * @param <E>  the key type for edge identifiers
 * @param <EL> the value type of edge label
 * @param <EP> the value type of edge properties
 * 
 */

public class GraphExtended<K, VL, VP, E, EL, EP> implements java.io.Serializable{

	// private final List<VertexExtended<K, VL, VP>> vertices;
	private HashMap<Long, VertexExtended<K, VL, VP>> vertices = new HashMap<>();
	private final HashMap<Long, EdgeExtended<E, K, EL, EP>> edges = new HashMap<>();
	private HashMap<String, KDTree> KDTreeSetVertex = new HashMap<>();
	private HashMap<String, KDTree> KDTreeSetEdge = new HashMap<>();

	/* initialization */
	private GraphExtended(List<VertexExtended<K, VL, VP>> vertices,
			List<EdgeExtended<E, K, EL, EP>> edges,
			Boolean Balanced_KDTree,
			Boolean EdgeProps,
			String path) throws ClassNotFoundException {

		for (EdgeExtended<E, K, EL, EP> e : edges) {
			Long id = (Long) e.getEdgeId();
			this.edges.put(id, e);
		}

		for (VertexExtended<K, VL, VP> v : vertices) {
			Long id = (Long) v.getVertexId();
			this.vertices.put(id, v);
		}
		InitializeKDTreeSet(vertices, edges, Balanced_KDTree, EdgeProps, path);
	}

	private void InitializeKDTreeSet(List<VertexExtended<K, VL, VP>> vertices, List<EdgeExtended<E, K, EL, EP>> edges,
			Boolean Balanced_KDTree, Boolean EdgeProps, String path) throws ClassNotFoundException {
		HashMap<String, List<Pair<String[], VertexExtended<K, VL, VP>>>> hashMap = new HashMap<>();
		
		// Read object from file if present
		String tarDir;
		if (Balanced_KDTree) {
			tarDir = path + "/KDTree/Balanced";
		}else{
			tarDir = path + "/KDTree/Unbalanced";
		}
		File theDir = new File(tarDir);
		if (theDir.exists()) {
			// theDir.mkdirs();
			try {
				ObjectInputStream objectInputStream = new ObjectInputStream(
						new FileInputStream(tarDir + "/kdtree.ser"));
				this.KDTreeSetVertex = (HashMap<String, KDTree>) objectInputStream.readObject();
			} catch (IOException ioException) {
				ioException.printStackTrace();
			}

		}
		 else {
			// Compute object from scratch if absent
			for (VertexExtended<K, VL, VP> vertex : vertices) {
				String label = (String) vertex.getLabel();

				String[] keys = getKeys((HashMap<String, String>) vertex.getProps(), "vertex");

				// Generated Hashmap of vertices
				if (Balanced_KDTree) {
					if (!hashMap.containsKey(label)) {
						hashMap.put(label, new ArrayList<>());
					}
					hashMap.get(label).add(new Pair<String[], VertexExtended<K, VL, VP>>(keys, vertex));
				} else {
					if (this.KDTreeSetVertex.containsKey(label)) {
						this.KDTreeSetVertex.get(label).insert(keys, vertex);
					} else {
						KDTree kd = new KDTree(keys.length);
						kd.insert(keys, vertex);
						this.KDTreeSetVertex.put(label, kd);
					}
				}
			}

			// Convert each arraylist in hashmap to object array
			if (Balanced_KDTree) {
				HashMap<String, Object[]> hashMapArray = new HashMap<>();
				for (String label : hashMap.keySet()) {
					hashMapArray.put(label, hashMap.get(label).toArray());
				}
				for (String label : hashMapArray.keySet()) {
					Pair<String[], String> p = (Pair<String[], String>) hashMapArray.get(label)[0];
					int dims = p.getValue0().length;
					KDTree kd = medianKDTree(hashMapArray.get(label), dims, label);
					this.KDTreeSetVertex.put(label, kd);
				}
				hashMap = null; // garbage collector
				hashMapArray = null;
			}
			// Wtite object to file if absent
			theDir.mkdirs();
			try{
				FileOutputStream fileOutputStream = new FileOutputStream(tarDir + "/kdtree.ser");
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
				//Writing the object 
				objectOutputStream.writeObject(this.KDTreeSetVertex);
				//Close the ObjectOutputStream
				objectOutputStream.close();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		

		for (EdgeExtended<E, K, EL, EP> edge : edges) {
			String label = (String) edge.getLabel();
			String id = edge.getEdgeId().toString();

			String[] keys = getKeys((HashMap<String, String>) edge.getProps(), "edge");
			keys[0] = id;

			if (this.KDTreeSetEdge.containsKey(label)) {
				this.KDTreeSetEdge.get(label).insert(keys, edge);
			} else {
				KDTree kd = new KDTree(keys.length);
				kd.insert(keys, edge);
				this.KDTreeSetEdge.put(label, kd);
			}
		}
	}

	public static KDTree medianKDTree(Object[] arr, int dims, String label) {
		KDTree kdTree = new KDTree(dims);
		QuickSelect medianObj = new QuickSelect();
		Queue<Triplet<Integer, Integer, Integer>> queue = new LinkedList<>();
		queue.add(new Triplet<Integer, Integer, Integer>(0, arr.length - 1, 0));
		Triplet<Integer, Integer, Integer> cur_args;

		ProgressBar pb = new ProgressBar(("Building Balanced KDTree for " + label), arr.length); // name, initial max
		pb.start();

		while (!queue.isEmpty()) {
			cur_args = queue.poll();
			Quartet<Integer, Integer, Integer, Object> quartet = medianObj.median(arr, cur_args.getValue0(),
					cur_args.getValue1(), cur_args.getValue2() % dims);

			Pair<String[], String> p;
			try {
				p = (Pair<String[], String>) quartet.getValue3();
			} catch (Exception e) {
				System.out.println("count");
				continue;
			}
			kdTree.insert(p.getValue0(), p.getValue1());
			pb.step();
			if (quartet.getValue0().compareTo(quartet.getValue2() - 1) <= 0) {
				queue.add(new Triplet<Integer, Integer, Integer>(quartet.getValue0(), quartet.getValue2() - 1,
						cur_args.getValue2() + 1));
			}
			if (quartet.getValue1().compareTo(quartet.getValue2() + 1) >= 0) {
				queue.add(new Triplet<Integer, Integer, Integer>(quartet.getValue2() + 1, quartet.getValue1(),
						cur_args.getValue2() + 1));
			}
		}

		pb.stop();

		return kdTree;
	}

	private String[] getKeys(HashMap<String, String> props, String type) {
		Set<String> keySet = props.keySet();

		ArrayList<String> sortedKeys = new ArrayList();
		if (type.equals("edge")) {
			sortedKeys.add("_id");
		}

		sortedKeys.addAll(new TreeSet(keySet));

		String KDKey[] = new String[sortedKeys.size()];

		for (int i = type.equals("edge") ? 1 : 0; i < sortedKeys.size(); i += 1) {
			KDKey[i] = props.get(sortedKeys.get(i));
		}
		return KDKey;
	}

	public HashMap<String, KDTree> getAllKDTrees(String type) {
		if (type.equals("edge")) {
			return this.KDTreeSetEdge;
		}
		return this.KDTreeSetVertex;
	}

	public KDTree getKDTreeVertexByLabel(String label) {
		return this.KDTreeSetVertex.get(label);
	}

	public KDTree getKDTreeEdgeByLabel(String label) {
		return this.KDTreeSetEdge.get(label);
	}

	public ArrayList<String> getPropKeySorted(String label, String type) {
		HashMap<String, String> props;
		if (type.equals("edge")) {
			props = (HashMap<String, String>) ((EdgeExtended) this.KDTreeSetEdge.get(label).getRoot()).getProps();
		} else {
			props = (HashMap<String, String>) ((VertexExtended) this.KDTreeSetVertex.get(label).getRoot()).getProps();

		}
		Set<String> keySet = props.keySet();
		return new ArrayList(new TreeSet(keySet));
	}

	public VertexExtended<K, VL, VP> getVertexByID(Long id) throws NoSuchElementException {
		try {
			return this.vertices.get(id);
		} catch (Exception e) {
			// TODO: handle exception
			throw new NoSuchElementException();
		}
	}

	/* get all edges in a graph */
	public Collection<EdgeExtended<E, K, EL, EP>> getEdges() {
		return this.edges.values();
	}

	/* get all vertices in a graph */
	public Collection<VertexExtended<K, VL, VP>> getVertices() {
		return this.vertices.values();
	}

	/* get all vertex IDs */
	public Set<Long> getAllVertexIds() {
		Set<Long> vertexIds = this.vertices.keySet();

		return vertexIds;
	}

	public Set<Long> getAllEdgeIds() {
		Set<Long> edgeIds = this.edges.keySet();
		return edgeIds;
	}

	public static <K, VL, VP, E, EL, EP> GraphExtended<K, VL, VP, E, EL, EP> fromList(
			List<VertexExtended<K, VL, VP>> vertices,
			List<EdgeExtended<E, K, EL, EP>> edges,
			Set<String> options,
			String path) throws ClassNotFoundException {
		Boolean balancedKdtree = false;
		Boolean edgeProps = false;
		if (options.contains("balanced_kdtree")) {
			balancedKdtree = true;
		} 
		if (options.contains("edge_properties")) {
			edgeProps = true;
		}
		return new GraphExtended<K, VL, VP, E, EL, EP>(vertices, edges, balancedKdtree, edgeProps, path);

	}

	public void deleteEdgeById(Long id) {
		this.edges.remove(id);
	}

	public void deleteVertexById(Long id) {
		this.vertices.remove(id);
	}
}
