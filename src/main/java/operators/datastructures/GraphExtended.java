package operators.datastructures;

import java.util.List;
import java.util.stream.Collectors;

import org.javatuples.Unit;

/**
 * Extended graph for Cypher Implementation
 * @param <K> the key type for vertex identifiers
 * @param <VL> the value type of vertex labels
 * @param <VP> the value type of vertex properties
 * @param <E> the key type for edge identifiers
 * @param <EL> the value type of edge label
 * @param <EP> the value type of edge properties
 * 
 */

public class GraphExtended<K, VL, VP, E, EL, EP> {
	
	/*adjacent lists might be added later*/
	private final List<VertexExtended<K, VL, VP>> vertices;
	private final List<EdgeExtended<E, K, EL, EP>> edges;
	
	/*initialization*/
	private GraphExtended(List<VertexExtended<K, VL, VP>> vertices, 
				  List<EdgeExtended<E, K, EL, EP>> edges) {
		this.vertices = vertices;
		this.edges = edges;
	}
	
	/*get all edges in a graph*/
	public List<EdgeExtended<E, K, EL, EP>> getEdges(){
		return this.edges;
	}
	
	/*get all vertices in a graph*/
	public List<VertexExtended<K, VL, VP>> getVertices(){
		return this.vertices;
	}
	
	/*get all vertex IDs*/
	public List<Unit<K>> getAllVertexIds() {
		List<Unit<K>> vertexIds = this.vertices.stream()
											   .map(elt -> Unit.with(elt.getVertexId()))
											   .collect(Collectors.toList());

		return vertexIds;
	}

	public List<Unit<E>> getAllEdgeIds() {
		List<Unit<E>> edgeIds = this.edges.stream()
											   .map(elt -> Unit.with(elt.getEdgeId()))
											   .collect(Collectors.toList());

		return edgeIds;
	}
	
	// public static <K, VL, VP, E, EL, EP> GraphExtended<K, VL, VP, E, EL, EP> 
	// 	fromCollection(Collection<VertexExtended<K, VL, VP>> vertices,
	// 		Collection<EdgeExtended<E, K, EL, EP>> edges) {

	// 	return fromDataSet(context.fromCollection(vertices),
	// 			context.fromCollection(edges));
	// }
	
	public static <K, VL, VP, E, EL, EP> GraphExtended<K, VL, VP, E, EL, EP> 
		fromDataSet(List<VertexExtended<K, VL, VP>> vertices,
			List<EdgeExtended<E, K, EL, EP>> edges) {

		return new GraphExtended<K, VL, VP, E, EL, EP>(vertices, edges);
	}
	/*NOT FINISHED YET*/
}
