package operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import operators.booleanExpressions.FilterOutEdgesByBooleanExpressions;
import operators.datastructures.*;
import operators.helper.FilterFunction;

/*
 * UnaryOperators class includes all unary operators:
 * 1. edge-join operator
 * 2. projection operator
 * 3. vertex-join operator (not mentioned in the thesis. In the thesis and the
 * implementation of data generator, when we connect vertices, we usually use a
 * scan operator to select the vertex and then a join operator. Here the
 * vertex-join operator combines these two functions.)
 *
 */
public class UnaryOperators {

	// Input graph
	private GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph;

	// Each list contains the vertex IDs and edge IDs of a selected path so far
	private List<List<Long>> paths;

	// Get the input graph, current columnNumber and the vertex and edges IDs
	public UnaryOperators(
			GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> g,
			List<List<Long>> paths) {
		this.graph = g;
		this.paths = paths;
	}

	// No specific queries on the current vertices
	public List<List<Long>> selectVertices() {
		return paths;
	}

	// No specific requirements on selected edges on right side
	public List<EdgeExtended<Long, Long, String, HashMap<String, String>>> selectOutEdges(Long sourceVertexID) {

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> edgeIDs = graph
				.getEdges()
				.stream()
				.filter(e -> e.getSourceId().equals(sourceVertexID))
				.collect(Collectors.toList());
		return edgeIDs;
	}

	// Select outgoing edges by boolean expressions
	public List<List<Long>> selectOutEdgesByBooleanExpressions(int col,
			FilterFunction<EdgeExtended<Long, Long, String, HashMap<String, String>>> filterEdges,
			FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> filterVertices) {

		FilterOutEdgesByBooleanExpressions filterEdg = new FilterOutEdgesByBooleanExpressions(filterEdges,
				filterVertices);

		List<List<Long>> selectedResults = this.paths.parallelStream().map(list -> {
			List<EdgeExtended<Long, Long, String, HashMap<String, String>>> outEdges = this
					.selectOutEdges(list.get(col)) // outgoing edges from vertex at col
					.stream()
					.filter(e -> {
						try {
							VertexExtended<Long, HashSet<String>, HashMap<String, String>> v = graph.getVertexByID(e.getTargetId());
							return filterEdg.join(e, v);
						} catch (Exception e1) {
							e1.printStackTrace();
							return false;
						}
					})
					.collect(Collectors.toList());

			List<List<Long>> intermediateList = new ArrayList<>();
			for (EdgeExtended<Long, Long, String, HashMap<String, String>> outE : outEdges) {
				List<Long> cloned_list = new ArrayList<Long>(list);
				cloned_list.add(outE.getEdgeId());
				cloned_list.add(outE.getTargetId());
				intermediateList.add(cloned_list);
			}
			return intermediateList;
		}).flatMap(s -> s.stream())
				.collect(Collectors.toList());

		this.paths = selectedResults;
		return selectedResults;
	}

	// No specific requirements on selected edges on left side
	public List<EdgeExtended<Long, Long, String, HashMap<String, String>>> selectInEdges(Long targetVertexID) {

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> edgeIDs = graph
				.getEdges()
				.stream()
				.filter(e -> e.getTargetId().equals(targetVertexID))
				.collect(Collectors.toList());
		return edgeIDs;
	}

	// Select ingoing edges by boolean expressions
	public List<List<Long>> selectInEdgesByBooleanExpressions(int col,
			FilterFunction<EdgeExtended<Long, Long, String, HashMap<String, String>>> filterEdges,
			FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> filterVertices) {

		FilterOutEdgesByBooleanExpressions filterEdg = new FilterOutEdgesByBooleanExpressions(filterEdges,
				filterVertices);

		List<List<Long>> selectedResults = this.paths.parallelStream().map(list -> {

			List<EdgeExtended<Long, Long, String, HashMap<String, String>>> inEdges = this
					.selectInEdges(list.get(col))
					.stream()
					.filter(e -> {
						try {
							return filterEdg.join(e, graph.getVertexByID(e.getSourceId()));
						} catch (Exception e1) {
							e1.printStackTrace();
							return true;
						}
					})
					.collect(Collectors.toList());

			List<List<Long>> intermediateList = new ArrayList<>();
			for (EdgeExtended<Long, Long, String, HashMap<String, String>> inE : inEdges) {
				List<Long> cloned_list = new ArrayList<Long>(list);
				cloned_list.add(inE.getEdgeId());
				cloned_list.add(inE.getSourceId());
				intermediateList.add(cloned_list);
			}
			return intermediateList;
		})
				.flatMap(s -> s.stream())
				.collect(Collectors.toList());

		this.paths = selectedResults;
		return selectedResults;
	}
}