package operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;

import org.apache.flink.api.common.functions.CoGroupFunction;
// import org.apache.flink.api.common.functions.FlatJoinFunction;
import operators.helper.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.List;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeList;
import org.apache.flink.api.java.tuple.Pair;
import org.apache.flink.util.Collector;
@SuppressWarnings("serial")


/*
* A more efficient way to implement label matching operator by using delta iterator in Flink
* So far only work with non-circle query pattern
* */
public class LabelMatchingOperatorsBeta {
	//Input graph
	private final GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long,
	  String, HashMap<String, String>> graph;
		
	//Each list contains the vertex IDs and edge IDs of a selected path so far 
	private List<ArrayList<Long>> paths;

	//Get the input graph, current columnNumber and the vertex and edges IDs
	public LabelMatchingOperatorsBeta(GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long,
			  String, HashMap<String, String>> graph,
			  List<ArrayList<Long>> paths) {
		this.graph = graph;
		this.paths = paths;
	}
	
    //Only the one with upper bound is implemented
	public List<ArrayList<Long>> matchWithUpperBound(int col, int ub, String label, JoinHint strategy) throws Exception {
		//Initial WorkSet List consisting of vertex-pair IDs for Delta Iteration. Each field of Pair<Long, Long> stores two same IDs since these two are starting vertices
		List<Pair<Long, Pair<Long, Long>>> initialSolutionSet = this.paths
				.map(new ExtractVertexIds(col));

		int maxIterations = ub;

		List<Pair<Long, Pair<Long, Long>>> initialWorkSet = initialSolutionSet;

		DeltaIteration<Pair<Long, Pair<Long, Long>>, Pair<Long, Pair<Long, Long>>> iteration = initialSolutionSet.iterateDelta(initialWorkSet, maxIterations, 1);
		List<Pair<Long, Pair<Long, Long>>> nextWorkset = iteration
				.getWorkset()
				.join(graph.getEdges(), strategy)
				.where(0)
				.equalTo(1)
				.with(new FilterEdgesByLabel(label));

		List<Pair<Long, Pair<Long, Long>>> deltas = iteration
				.getSolutionSet()
				.coGroup(nextWorkset)
				.where(1)
				.equalTo(1)
                .with(new GetNewResults());

		List<Pair<Long, Pair<Long, Long>>> mergedResults = iteration.closeWith(deltas, nextWorkset);

	    KeySelectorForColumns verticesSelector = new KeySelectorForColumns(col);
	    List<ArrayList<Long>> results = this.paths
				.join(mergedResults.map(new ExtractVertexPairs()))
				.where(verticesSelector)
				.equalTo(0)
				.with(new UpdateVertexAndEdgeIds());

		this.paths = results;

		return results;

	}

	private static class ExtractVertexIds implements MapFunction<ArrayList<Long>,
			Pair<Long, Pair<Long, Long>>>{
		private int col = 0;
		ExtractVertexIds(int column) { this.col = column; }
		@Override
		public Pair<Long, Pair<Long, Long>> map(ArrayList<Long> idsOfVerticesAndEdges)
				throws Exception {
			return new Pair<Long, Pair<Long, Long>>(idsOfVerticesAndEdges.get(col), new Pair<Long, Long>(idsOfVerticesAndEdges.get(col), idsOfVerticesAndEdges.get(col)));
		}
	}

	private static class FilterEdgesByLabel implements FlatJoinFunction<Pair<Long, Pair<Long, Long>>, EdgeExtended<Long, Long, String, HashMap<String, String>>,
		Pair<Long, Pair<Long, Long>>> {
		private String label;

		public FilterEdgesByLabel(String label) {this.label = label;}

		@Override
		public void join(
				Pair<Long, Pair<Long, Long>> vertexIds,
				EdgeExtended<Long, Long, String, HashMap<String, String>> edge,
				Collector<Pair<Long, Pair<Long, Long>>> vertices)
				throws Exception {
			if(edge.f3.equals(this.label)){
				Pair<Long, Long> result = new Pair<>(vertexIds.f1.f0, edge.f2);
				vertices.collect(new Pair<Long, Pair<Long, Long>>(edge.f2, result));
			}
		}
    }

	private static class GetNewResults implements CoGroupFunction<Pair<Long, Pair<Long, Long>>, Pair<Long, Pair<Long, Long>>, Pair<Long, Pair<Long, Long>>> {

		@Override
		public void coGroup(Iterable<Pair<Long, Pair<Long, Long>>> originalVertices,
				Iterable<Pair<Long, Pair<Long, Long>>> newResults,
				Collector<Pair<Long, Pair<Long, Long>>> vertices)
				throws Exception {
			HashSet<Pair<Long, Pair<Long, Long>>> prevVertices = new HashSet<>();
			for (Pair<Long, Pair<Long, Long>> prev : originalVertices) {
				prevVertices.add(prev);
			}
			for (Pair<Long, Pair<Long, Long>> next: newResults) {
				if (!prevVertices.contains(next)) {
					vertices.collect(next);
				}
			}
		}
	}

	private static class UpdateVertexAndEdgeIds implements FlatJoinFunction<ArrayList<Long>, Pair<Long, Long>, ArrayList<Long>> {

		@Override
		public void join(ArrayList<Long> vertexAndEdgeIds,
				Pair<Long, Long> vertexIds,
				Collector<ArrayList<Long>> updateIdsList) throws Exception {
			vertexAndEdgeIds.add(vertexIds.f1);
			updateIdsList.collect(vertexAndEdgeIds);
		}
	}

	private static class ExtractVertexPairs implements MapFunction<Pair<Long, Pair<Long, Long>>, Pair<Long, Long>> {

		@Override
		public Pair<Long, Long> map(Pair<Long, Pair<Long, Long>> vertexIds)
				throws Exception {
			return vertexIds.f1;
		}

	}
}
