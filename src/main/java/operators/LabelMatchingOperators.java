// package operators;

// import java.util.ArrayList;
// import java.util.HashSet;
// import java.util.List;
// import java.util.HashMap;

// import operators.datastructures.EdgeExtended;
// import operators.datastructures.GraphExtended;

// import org.apache.flink.api.common.functions.CoGroupFunction;

// import operators.helper.Collector;
// // import org.apache.flink.api.common.functions.FlatJoinFunction;
// import operators.helper.FlatJoinFunction;
// import org.apache.flink.api.common.functions.GroupReduceFunction;
// // import org.apache.flink.api.common.functions.MapFunction;
// import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
// // import org.apache.flink.api.java.List;
// import org.apache.flink.api.java.operators.IterativeList;
// // import org.apache.flink.api.java.tuple.Pair;
// // import org.apache.flink.util.Collector;
// import org.javatuples.Pair;

// /*
//  * Label matching operators are implemented here:
//  * (1）Define both the upper bound and the lower bound of the number of edges on the paths
//  * (2) Define the upper bound of the number of edges on the paths
//  * (3) Define the lower bound of the number of edges on the paths
//  * (4) Do not define any bounds
//  * */
// @SuppressWarnings("serial")
// public class LabelMatchingOperators {
// 	// Input graph
// 	private final GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph;

// 	// Each list contains the vertex IDs and edge IDs of a selected path so far
// 	private List<Long> paths;

// 	// Get the input graph, current columnNumber and the vertex and edges IDs
// 	public LabelMatchingOperators(
// 			GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph,
// 			List<Long> paths) {
// 		this.graph = graph;
// 		this.paths = paths;
// 	}

// 	// define both upper bound and lower bound of the number of edges traversed
// 	public List<ArrayList<Long>> matchWithBounds(int col, int lb, int ub, String label, JoinHint strategy)
// 			throws Exception {

// 		List<Pair<Long, Long>> verticesWorkset = this.paths
// 				.map(new ExtractVertexIds(col));

// 		int minIterations = lb;
// 		int maxIterations = ub;
// 		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
// 		IterativeList<Pair<Long, Long>> getInitialWorkset = verticesWorkset.iterate(minIterations);

// 		List<Pair<Long, Long>> initialResults = getInitialWorkset
// 				.join(graph.getEdges(), strategy)
// 				.where(1)
// 				.equalTo(1)
// 				.with(new FilterEdgesByLabel(label));

// 		List<Pair<Long, Long>> initialWorkset = getInitialWorkset
// 				.closeWith(initialResults)
// 				.map(new GetStartingVertexIds())
// 				.groupBy(0, 1)
// 				.reduceGroup(new DuplicatesReduction());

// 		if (ub == lb) {
// 			List<ArrayList<Long>> results = this.paths
// 					.join(initialWorkset)
// 					.where(verticesSelector)
// 					.equalTo(0)
// 					.with(new UpdateVertexAndEdgeIds());
// 			this.paths = results;
// 			return results;
// 		} else {
// 			IterativeList<Pair<Long, Long>> iteration = initialWorkset.iterate(maxIterations);

// 			List<Pair<Long, Long>> nextResults = iteration
// 					.join(graph.getEdges(), strategy)
// 					.where(1)
// 					.equalTo(1)
// 					.with(new FilterEdgesByLabel(label))
// 					.union(iteration)
// 					.groupBy(0, 1)
// 					.reduceGroup(new DuplicatesReduction())
// 					.withForwardedFields("0;1");

// 			// Using this coGroup to quickly detect whether new vertex pairs are added, if
// 			// not, terminate the iterations
// 			List<Pair<Long, Long>> newResults = iteration
// 					.coGroup(nextResults)
// 					.where(0)
// 					.equalTo(0)
// 					.with(new GetNewResults())
// 					.withForwardedFieldsFirst("0")
// 					.withForwardedFieldsSecond("0");

// 			List<Pair<Long, Long>> mergedResults = iteration.closeWith(nextResults, newResults);

// 			List<ArrayList<Long>> results = this.paths
// 					.join(mergedResults)
// 					.where(verticesSelector)
// 					.equalTo(0)
// 					.with(new UpdateVertexAndEdgeIds());

// 			this.paths = results;
// 			return results;
// 		}
// 	}

// 	// define upper bound and lower bound of the number of edges traversed
// 	public List<ArrayList<Long>> matchWithUpperBound(int col, int ub, String label, JoinHint strategy)
// 			throws Exception {
// 		// Initial WorkSet List consisting of vertex-pair IDs for Delta Iteration. Each
// 		// field of Pair<Long, Long> stores two same IDs since these two are starting
// 		// vertices
// 		List<Pair<Long, Long>> verticesWorkset = this.paths
// 				.map(new ExtractVertexIds(col));

// 		int maxIterations = ub;
// 		IterativeList<Pair<Long, Long>> iteration = verticesWorkset.iterate(maxIterations);

// 		List<Pair<Long, Long>> nextResults = iteration
// 				.join(graph.getEdges(), strategy)
// 				.where(1)
// 				.equalTo(1)
// 				.with(new FilterEdgesByLabel(label))
// 				.union(iteration)
// 				.groupBy(0, 1)
// 				.reduceGroup(new DuplicatesReduction())
// 				.withForwardedFields("0;1");

// 		List<Pair<Long, Long>> newResults = iteration
// 				.coGroup(nextResults)
// 				.where(0)
// 				.equalTo(0)
// 				.with(new GetNewResults())
// 				.withForwardedFieldsFirst("0")
// 				.withForwardedFieldsSecond("0");

// 		List<Pair<Long, Long>> mergedResults = iteration.closeWith(nextResults, newResults);

// 		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
// 		List<ArrayList<Long>> results = this.paths
// 				.join(mergedResults)
// 				.where(verticesSelector)
// 				.equalTo(0)
// 				.with(new UpdateVertexAndEdgeIds());

// 		this.paths = results;
// 		return results;
// 	}

// 	// define lower bound of the number of edges traversed
// 	public List<ArrayList<Long>> matchWithLowerBound(int col, int lb, String label, JoinHint strategy)
// 			throws Exception {

// 		List<Pair<Long, Long>> verticesWorkset = this.paths
// 				.map(new ExtractVertexIds(col));
// 		int minIterations = lb;
// 		int maxIterations = 1000;

// 		IterativeList<Pair<Long, Long>> getInitialWorkset = verticesWorkset.iterate(minIterations);

// 		List<Pair<Long, Long>> initialResults = getInitialWorkset
// 				.join(graph.getEdges(), strategy)
// 				.where(1)
// 				.equalTo(1)
// 				.with(new FilterEdgesByLabel(label));

// 		List<Pair<Long, Long>> initialWorkset = getInitialWorkset
// 				.closeWith(initialResults)
// 				.map(new GetStartingVertexIds())
// 				.groupBy(0, 1)
// 				.reduceGroup(new DuplicatesReduction());

// 		IterativeList<Pair<Long, Long>> iteration = initialWorkset.iterate(maxIterations);

// 		List<Pair<Long, Long>> nextResults = iteration
// 				.join(graph.getEdges(), strategy)
// 				.where(1)
// 				.equalTo(1)
// 				.with(new FilterEdgesByLabel(label))
// 				.union(iteration)
// 				.groupBy(0, 1)
// 				.reduceGroup(new DuplicatesReduction())
// 				.withForwardedFields("0;1");

// 		// Using this coGroup to quickly detect whether new vertex pairs are added, if
// 		// not, terminate the iterations
// 		List<Pair<Long, Long>> newResults = iteration
// 				.coGroup(nextResults)
// 				.where(0)
// 				.equalTo(0)
// 				.with(new GetNewResults())
// 				.withForwardedFieldsFirst("0")
// 				.withForwardedFieldsSecond("0");

// 		List<Pair<Long, Long>> mergedResults = iteration.closeWith(nextResults, newResults);

// 		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
// 		List<ArrayList<Long>> results = this.paths
// 				.join(mergedResults)
// 				.where(verticesSelector)
// 				.equalTo(0)
// 				.with(new UpdateVertexAndEdgeIds());

// 		this.paths = results;
// 		return results;

// 	}

// 	// Do not define any bounds
// 	public List<ArrayList<Long>> matchWithoutBounds(int col, String label, JoinHint strategy) throws Exception {
// 		// Initial WorkSet List consisting of vertex-pair IDs for Delta Iteration Each
// 		// field of Pair<Long, Long> stores two same IDs since these two are starting
// 		// vertices
// 		List<Pair<Long, Long>> verticesWorkset = this.paths
// 				.map(new ExtractVertexIds(col));

// 		int maxIterations = 100000;

// 		IterativeList<Pair<Long, Long>> iteration = verticesWorkset.iterate(maxIterations);

// 		List<Pair<Long, Long>> nextResults = iteration
// 				.join(graph.getEdges(), strategy)
// 				.where(1)
// 				.equalTo(1)
// 				.with(new FilterEdgesByLabel(label))
// 				.union(iteration)
// 				.groupBy(0, 1)
// 				.reduceGroup(new DuplicatesReduction())
// 				.withForwardedFields("0;1");

// 		// Using this coGroup to quickly detect whether new vertex pairs are added, if
// 		// not, terminate the iterations
// 		List<Pair<Long, Long>> newResults = iteration
// 				.coGroup(nextResults)
// 				.where(0)
// 				.equalTo(0)
// 				.with(new GetNewResults())
// 				.withForwardedFieldsFirst("0")
// 				.withForwardedFieldsSecond("0");

// 		List<Pair<Long, Long>> mergedResults = iteration.closeWith(nextResults, newResults);

// 		KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
// 		List<ArrayList<Long>> results = this.paths
// 				.join(mergedResults)
// 				.where(verticesSelector)
// 				.equalTo(0)
// 				.with(new UpdateVertexAndEdgeIds());

// 		this.paths = results;
// 		return results;
// 	}

// 	private static class ExtractVertexIds implements MapFunction<ArrayList<Long>, Pair<Long, Long>> {
// 		private int col = 0;

// 		ExtractVertexIds(int column) {
// 			this.col = column;
// 		}

// 		@Override
// 		public Pair<Long, Long> map(ArrayList<Long> idsOfVerticesAndEdges)
// 				throws Exception {
// 			return new Pair<Long, Long>(idsOfVerticesAndEdges.get(col), idsOfVerticesAndEdges.get(col));
// 		}
// 	}

// 	private static class FilterEdgesByLabel implements
// 			FlatJoinFunction<Pair<Long, Long>, EdgeExtended<Long, Long, String, HashMap<String, String>>, Pair<Long, Long>> {

// 		private String lab = "";

// 		FilterEdgesByLabel(String label) {
// 			this.lab = label;
// 		}

// 		@Override
// 		public void join(
// 				Pair<Long, Long> vertexIds,
// 				EdgeExtended<Long, Long, String, HashMap<String, String>> edge,
// 				Collector<Pair<Long, Long>> out) throws Exception {
// 			if (edge.getLabel().equals(lab))
// 				out.collect(new Pair<Long, Long>(vertexIds.getValue0(), edge.getTargetId()));
// 		}
// 	}

// 	private static class GetStartingVertexIds implements MapFunction<Pair<Long, Long>, Pair<Long, Long>> {

// 		@Override
// 		public Pair<Long, Long> map(Pair<Long, Long> vertexIds)
// 				throws Exception {

// 			return new Pair<Long, Long>(vertexIds.getValue0(), vertexIds.getValue1());
// 		}

// 	}

// 	private static class DuplicatesReduction implements GroupReduceFunction<Pair<Long, Long>, Pair<Long, Long>> {
// 		@Override
// 		public void reduce(Iterable<Pair<Long, Long>> vertexIds, Collector<Pair<Long, Long>> out) {
// 			out.collect(vertexIds.iterator().next());
// 		}
// 	}

// 	private static class GetNewResults
// 			implements CoGroupFunction<Pair<Long, Long>, Pair<Long, Long>, Pair<Long, Long>> {
// 		@Override
// 		public void coGroup(Iterable<Pair<Long, Long>> originalVertexIds,
// 				Iterable<Pair<Long, Long>> newVertexIds,
// 				Collector<Pair<Long, Long>> results) throws Exception {
// 			HashSet<Pair<Long, Long>> prevResults = new HashSet<Pair<Long, Long>>();
// 			for (Pair<Long, Long> prev : originalVertexIds) {
// 				prevResults.add(prev);
// 			}
// 			for (Pair<Long, Long> next : newVertexIds) {
// 				if (!prevResults.contains(next)) {
// 					results.collect(next);
// 				}
// 			}
// 		}
// 	}

// 	private static class UpdateVertexAndEdgeIds
// 			implements FlatJoinFunction<ArrayList<Long>, Pair<Long, Long>, ArrayList<Long>> {
// 		@Override
// 		public void join(
// 				ArrayList<Long> vertexAndEdgeIds,
// 				Pair<Long, Long> vertexIds,
// 				Collector<ArrayList<Long>> updateIdsList) throws Exception {
// 			// vertexAndEdgeIds.add(Long.MAX_VALUE);
// 			vertexAndEdgeIds.add(vertexIds.getValue1());
// 			updateIdsList.collect(vertexAndEdgeIds);
// 		}
// 	}

// }
