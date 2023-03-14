package operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;
import operators.helper.FilterFunction;

// import org.apache.flink.api.java.List;
// import org.apache.flink.api.common.functions.MapFunction;

/*
* A scan operator is used to extract all vertex IDs which satisfy certain filtering conditions.
* The filtering conditions could be:
* (1) no conditions
* (2) filtering conditions on labels of vertices
* (3) filtering conditions on properties of vertices
* (4) a combination of conditions related by complex boolean expressions
* */
public class ScanOperators {
	private final GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph;

	// Get the input graph
	public ScanOperators(
			GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> g) {
		this.graph = g;
	}

	// Get vertex IDs of a graph
	public Set<Long> getInitialVertices() {
		Set<Long> vertexIds = graph.getAllVertexIds();
		return vertexIds;
	}

	// Extract vertex IDs into ArrayList from vertices
	// private static class InitialVerticesToLists {
	// public ArrayList<Long> map(
	// VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex) throws
	// Exception {
	// ArrayList<Long> row = new ArrayList<>();
	// row.add(vertex.getVertexId());
	// return row;
	// }
	// }

	// Get edge IDs of a graph
	// not very useful so far
	public List<Long> getInitialEdges() {
		List<Long> edgeIds = graph.getAllEdgeIds();
		return edgeIds;
	}

	// Extract edge IDs into ArrayList from edges
	// also not very useful so far
	// private static class InitialEdgesToLists {
	// public ArrayList<Long> map(EdgeExtended<Long, Long, String, HashMap<String,
	// String>> edge) throws Exception {
	// ArrayList<Long> row = new ArrayList<>();
	// row.add(edge.getEdgeId());
	// return row;
	// }
	// }

	// Get vertex ids with label constraints
	public List<Long> getInitialVerticesByLabels(String labels) {
		List<Long> vertexIds = graph
				.getVertices()
				.stream()
				.filter(v -> v.getLabel().equals(labels))
				.map(v -> v.getVertexId())
				.collect(Collectors.toList());
		return vertexIds;
	}

	// Get vertex IDs with property constraints
	// Check whether all properties specified in the query are existing the
	// corresponding values of certain properties are consistent with ones in the
	// query
	public List<Long> getInitialVerticesByProperties(HashMap<String, String> properties) {
		List<Long> vertexIds = graph
				.getVertices()
				.stream()
				.filter(v -> {
					for (Map.Entry<String, String> propInQuery : properties.entrySet()) {
						// If the vertex does not contain the specific key
						if (v.getProps().get(propInQuery.getKey()) == null ||
						// If the key is contained, check if the value is consistent or not
								!v.getProps().get(propInQuery.getKey()).equals(propInQuery.getValue())) {
							return false;
						}
					}
					return true;
				})
				.map(v -> v.getVertexId())
				.collect(Collectors.toList());
		return vertexIds;
	}

	// Get vertex IDs filtered by a combination of conditions related by complex
	// boolean expressions
	public List<List<Long>> getInitialVerticesByBooleanExpressions(
			FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> filterVertices) {

		List<List<Long>> vertexIds = graph
				.getVertices()
				.stream()
				.filter(v -> {
					try {

						return filterVertices.filter(v);
					} catch (Exception e) {
						// Auto-generated catch block
						// e.printStackTrace();
						return false;
					}
				})
				.map(v -> new ArrayList<>(Arrays.asList(v.getVertexId())))
				.collect(Collectors.toList());
		return vertexIds;
	}
}