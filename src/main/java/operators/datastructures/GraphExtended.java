package operators.datastructures;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import operators.datastructures.kdtree.KDTree;

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

public class GraphExtended<K, VL, VP, E, EL, EP> {

	// private final List<VertexExtended<K, VL, VP>> vertices;
	private HashMap<Long, VertexExtended<K, VL, VP>> vertices = new HashMap<>();
	private final HashMap<Long, EdgeExtended<E, K, EL, EP>> edges = new HashMap<>();
	private HashMap<String, KDTree> KDTreeSetVertex = new HashMap<>();
	private HashMap<String, KDTree> KDTreeSetEdge = new HashMap<>();

	/* initialization */
	private GraphExtended(List<VertexExtended<K, VL, VP>> vertices,
			List<EdgeExtended<E, K, EL, EP>> edges) {

		for (EdgeExtended<E, K, EL, EP> e : edges) {
			Long id = (Long) e.getEdgeId();
			this.edges.put(id, e);
		}

		for (VertexExtended<K, VL, VP> v : vertices) {
			Long id = (Long) v.getVertexId();
			this.vertices.put(id, v);
		}
		InitializeKDTreeSet(vertices, edges);
	}

	private void InitializeKDTreeSet(List<VertexExtended<K, VL, VP>> vertices, List<EdgeExtended<E, K, EL, EP>> edges) {
		for (VertexExtended<K, VL, VP> vertex : vertices) {
			String label = (String) vertex.getLabel();

			String[] keys = getKeys((HashMap<String, String>) vertex.getProps(), "vertex");

			if (this.KDTreeSetVertex.containsKey(label)) {
				this.KDTreeSetVertex.get(label).insert(keys, vertex);
			} else {
				KDTree kd = new KDTree(keys.length);
				kd.insert(keys, vertex);
				this.KDTreeSetVertex.put(label, kd);
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

	public ArrayList<String> getPropKeySorted(String label) {
		HashMap<String, String> props;
		try {
			props = (HashMap<String, String>) ((VertexExtended) this.KDTreeSetVertex.get(label).getRoot()).getProps();
		} catch (Exception e) {
			props = (HashMap<String, String>) ((EdgeExtended) this.KDTreeSetEdge.get(label).getRoot()).getProps();
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
			List<EdgeExtended<E, K, EL, EP>> edges) {

		return new GraphExtended<K, VL, VP, E, EL, EP>(vertices, edges);
	}

	public void deleteEdgeById(Long id) {
		this.edges.remove(id);
	}

	public void deleteVertexById(Long id) {
		this.vertices.remove(id);
	}
}
