package operators.datastructures;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeSet;

import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Triplet;

import com.google.gson.reflect.TypeToken;

import me.tongfei.progressbar.ProgressBar;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import operators.datastructures.kdtree.KDTree;
import operators.datastructures.kdtree.QuickSelect;

public class GraphExtended<K, VL, VP, E, EL, EP> implements java.io.Serializable {

	public class SerClass implements Serializable {
		private static final long serialVersionUID = 1234567L;

		HashMap<String, KDTree> hashMap;

		public SerClass(HashMap<String, KDTree> hm) {
			this.hashMap = hm;
		}
	}

	// private final List<VertexExtended<K, VL, VP>> vertices;
	private HashMap<Long, VertexExtended<K, VL, VP>> vertices = new HashMap<>();
	private final HashMap<Long, EdgeExtended> edges = new HashMap<>();
	private SerClass KDTreeSetVertex = new SerClass(new HashMap<>());
	private SerClass KDTreeSetEdge = new SerClass(new HashMap<>());

	/* initialization */
	private GraphExtended(List<VertexExtended<K, VL, VP>> vertices,
			List<EdgeExtended> edges,
			Boolean Balanced_KDTree,
			Boolean EdgeProps,
			String path) throws ClassNotFoundException {

		for (EdgeExtended e : edges) {
			Long id = (Long) e.getEdgeId();
			this.edges.put(id, e);
		}

		for (VertexExtended<K, VL, VP> v : vertices) {
			Long id = (Long) v.getVertexId();
			this.vertices.put(id, v);
		}
		InitializeKDTreeSet(vertices, edges, Balanced_KDTree, EdgeProps, path);
	}

	private void InitializeKDTreeSet(List<VertexExtended<K, VL, VP>> vertices, List<EdgeExtended> edges,
			Boolean Balanced_KDTree, Boolean EdgeProps, String path) throws ClassNotFoundException {
		HashMap<String, List<Pair<String[], VertexExtended<K, VL, VP>>>> hashMap = new HashMap<>();
		HashMap<String, List<Pair<String[], EdgeExtended>>> hashMap_edge = new HashMap<>();

		Type fooType = new TypeToken<HashMap<String, KDTree>>() {
		}.getType();

		// Read object from file if present
		Boolean vertexBool = false, edgeBool = false;

		String tarDir = path + "/KDTree/";
		String str1;
		if (Balanced_KDTree) {
			str1 = "balanced";
		} else {
			str1 = "unbalanced";
		}
		File theDir = new File(tarDir);

		if (theDir.exists()) {
			try {
				File file1 = new File(tarDir + "vertex/" + str1);
				if (file1.exists()) {
					this.KDTreeSetVertex.hashMap = ReadObjectFromFile(tarDir + "vertex/" + str1);
					// Gson gson = new GsonBuilder().setPrettyPrinting().create();
					// this.KDTreeSetVertex = gson.fromJson(new FileReader(tarDir + "vertex/" +
					// str1), fooType);

					vertexBool = true;
				}

				File file2 = new File(tarDir + "edge/" + str1);
				if (file2.exists()) {
					this.KDTreeSetEdge.hashMap = ReadObjectFromFile(tarDir + "edge/" + str1);
					// Gson gson = new GsonBuilder().setPrettyPrinting().create();
					// this.KDTreeSetEdge = gson.fromJson(new FileReader(tarDir + "edge/" + str1),
					// fooType);

					edgeBool = true;
				}
			} catch (IOException ioException) {
				ioException.printStackTrace();
			}

		}
		if (!vertexBool) {
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
					if (this.KDTreeSetVertex.hashMap.containsKey(label)) {
						this.KDTreeSetVertex.hashMap.get(label).insert(keys, vertex);
					} else {
						KDTree kd = new KDTree(keys.length);
						kd.insert(keys, vertex);
						this.KDTreeSetVertex.hashMap.put(label, kd);
					}
				}
			}
		}
		if (!edgeBool) {

			for (EdgeExtended edge : edges) {
				String label = (String) edge.getLabel();
				String id = edge.getEdgeId().toString();

				String[] keys = getKeys((HashMap<String, String>) edge.getProps(), "edge");
				keys[0] = id;

				if (Balanced_KDTree) {
					if (!hashMap_edge.containsKey(label)) {
						hashMap_edge.put(label, new ArrayList<>());
					}
					hashMap_edge.get(label).add(new Pair<String[], EdgeExtended>(keys, edge));
				} else {
					if (this.KDTreeSetEdge.hashMap.containsKey(label)) {
						this.KDTreeSetEdge.hashMap.get(label).insert(keys, edge);
					} else {
						KDTree kd = new KDTree(keys.length);
						kd.insert(keys, edge);
						this.KDTreeSetEdge.hashMap.put(label, kd);
					}
				}
			}
		}

		if (!vertexBool || !edgeBool) {

			// Convert each arraylist in hashmap to object array
			if (Balanced_KDTree) {
				HashMap<String, Object[]> hashMapArray = new HashMap<>();

				if (!vertexBool) {
					for (String label : hashMap.keySet()) {
						hashMapArray.put(label, hashMap.get(label).toArray());
					}
					for (String label : hashMapArray.keySet()) {
						Pair<String[], String> p = (Pair<String[], String>) hashMapArray.get(label)[0];
						int dims = p.getValue0().length;
						KDTree kd = medianKDTree(hashMapArray.get(label), dims, label);
						this.KDTreeSetVertex.hashMap.put(label, kd);
					}
					hashMap = null; // garbage collector
					hashMapArray = new HashMap<>();
				}

				if (!edgeBool) {
					// For Edge
					for (String label : hashMap_edge.keySet()) {
						hashMapArray.put(label, hashMap_edge.get(label).toArray());
					}
					for (String label : hashMapArray.keySet()) {
						Pair<String[], String> p = (Pair<String[], String>) hashMapArray.get(label)[0];
						int dims = p.getValue0().length;
						KDTree kd = medianKDTree(hashMapArray.get(label), dims, label);
						this.KDTreeSetEdge.hashMap.put(label, kd);
					}

					hashMap_edge = null; // garbage collector
					hashMapArray = null;
				}
			}

			// Wtite object to file if absent
			theDir.mkdirs();
			if (!vertexBool) {
				File f = new File(tarDir + "vertex/");
				f.mkdirs();

				WriteObjectToFile(this.KDTreeSetVertex, tarDir + "vertex/" + str1);
			}

			if (!edgeBool) {
				File f = new File(tarDir + "edge/");
				f.mkdirs();

				WriteObjectToFile(this.KDTreeSetEdge, tarDir + "edge/" + str1);
			}
		}
	}

	public void WriteObjectToFile(Object serObj, String filepath) {
		try {
			FileOutputStream fileOut = new FileOutputStream(filepath);
			ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
			objectOut.writeObject(serObj);
			objectOut.close();
			System.out.println("The Object  was succesfully written to a file");
		} catch (Exception ex) {
			// ex.printStackTrace();
			System.out.println("error");
		}
	}

	public HashMap<String, KDTree> ReadObjectFromFile(String filepath) throws IOException, ClassNotFoundException {
			FileInputStream fileIn = new FileInputStream(filepath);
			ObjectInputStream objectIn = new ObjectInputStream(fileIn);
			Object obj = objectIn.readObject();
			objectIn.close();
			return (HashMap<String, KDTree>)obj;
	}

	public static KDTree medianKDTree(Object[] arr, int dims, String label) {
		KDTree kdTree = new KDTree(dims);
		QuickSelect medianObj = new QuickSelect();
		Queue<Triplet<Integer, Integer, Integer>> queue = new LinkedList<>();
		queue.add(new Triplet<Integer, Integer, Integer>(0, arr.length - 1, 0));
		Triplet<Integer, Integer, Integer> cur_args;

		try (ProgressBar pb = new ProgressBar(("Building Balanced KDTree for " + label), arr.length)) {

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
		}

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
			return this.KDTreeSetEdge.hashMap;
		}
		return this.KDTreeSetVertex.hashMap;
	}

	public KDTree getKDTreeVertexByLabel(String label) {
		return this.KDTreeSetVertex.hashMap.get(label);
	}

	public KDTree getKDTreeEdgeByLabel(String label) {
		return this.KDTreeSetEdge.hashMap.get(label);
	}

	public ArrayList<String> getPropKeySorted(String label, String type) {
		HashMap<String, String> props;
		if (type.equals("edge")) {
			props = ((EdgeExtended) this.KDTreeSetEdge.hashMap.get(label).getRoot()).getProps();
		} else {
			props = (HashMap<String, String>) ((VertexExtended) this.KDTreeSetVertex.hashMap.get(label).getRoot()).getProps();
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
	public Collection<EdgeExtended> getEdges() {
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
			List<EdgeExtended> edges,
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