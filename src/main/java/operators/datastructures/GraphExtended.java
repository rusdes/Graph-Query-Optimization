package operators.datastructures;

import java.util.ArrayList;
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

	/* adjacent lists might be added later */
	private final List<VertexExtended<K, VL, VP>> vertices;
	private final List<EdgeExtended<E, K, EL, EP>> edges;
	private HashMap<String, KDTree> KDTreeSet = new HashMap<>();

	/* initialization */
	private GraphExtended(List<VertexExtended<K, VL, VP>> vertices,
			List<EdgeExtended<E, K, EL, EP>> edges) {
		this.vertices = vertices;
		this.edges = edges;
		InitializeKDTreeSet(vertices);
	}

	private void InitializeKDTreeSet(List<VertexExtended<K, VL, VP>> vertices){
		for(VertexExtended<K, VL, VP> vertex : vertices){
			String label = (String)vertex.getLabel();
			if(this.KDTreeSet.containsKey(label)){
				this.KDTreeSet.get(label).insert(getKeys((HashMap<String, String>)vertex.getProps()), vertex);
			}
			else{
				int dims = ((HashMap<String, String>) vertex.getProps()).size();
				KDTree kd = new KDTree(dims);
				kd.insert(getKeys((HashMap<String, String>)vertex.getProps()), vertex);
				this.KDTreeSet.put(label, kd);
			}
		}
	}

	private double[] getKeys(HashMap<String, String> props){
		Set<String> keySet = props.keySet();
		ArrayList<String> sortedKeys =  new ArrayList(new TreeSet(keySet));
		double KDKey[] = new double[sortedKeys.size()];

		for(int i = 0; i < sortedKeys.size(); i += 1){
			KDKey[i] = props.get(sortedKeys.get(i)).hashCode();
		}
		return KDKey;
	}

	public HashMap<String,KDTree> getAllKDTrees() {
		return this.KDTreeSet;
	}

	public KDTree getKDTreeByLabel(String label) {
		return this.KDTreeSet.get(label);
	}

	public ArrayList<String> getPropKeySorted(String label){
		HashMap <String,String> props = (HashMap <String,String>)((VertexExtended)this.KDTreeSet.get(label).getRoot()).getProps();
		Set<String> keySet = props.keySet();
		return new ArrayList(new TreeSet(keySet));
	}

	public VertexExtended<K, VL, VP> getVertexByID(Long id) throws NoSuchElementException{
		for (VertexExtended<K, VL, VP> vertex : vertices) {
			if(vertex.getVertexId() == id){
				return vertex;
			}
		}
		throw new NoSuchElementException();
	}
	/* get all edges in a graph */
	public List<EdgeExtended<E, K, EL, EP>> getEdges() {
		return this.edges;
	}

	/* get all vertices in a graph */
	public List<VertexExtended<K, VL, VP>> getVertices() {
		return this.vertices;
	}

	/* get all vertex IDs */
	public List<K> getAllVertexIds() {
		List<K> vertexIds = this.vertices.stream()
				.map(elt -> elt.getVertexId())
				.collect(Collectors.toList());

		return vertexIds;
	}

	public List<E> getAllEdgeIds() {
		List<E> edgeIds = this.edges.stream()
				.map(elt -> elt.getEdgeId())
				.collect(Collectors.toList());

		return edgeIds;
	}

	public static <K, VL, VP, E, EL, EP> GraphExtended<K, VL, VP, E, EL, EP> fromList(
			List<VertexExtended<K, VL, VP>> vertices,
			List<EdgeExtended<E, K, EL, EP>> edges) {

		return new GraphExtended<K, VL, VP, E, EL, EP>(vertices, edges);
	}
}
