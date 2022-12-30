package operators.booleanExpressions;

import java.util.ArrayList;
import java.util.HashMap;

import operators.datastructures.EdgeExtended;
import operators.helper.Collector;
import operators.helper.FilterFunction;

// import org.apache.flink.api.common.functions.FlatJoinFunction;
import operators.helper.FlatJoinFunction;
// import org.apache.flink.util.Collector;

public class FilterInEdgesByBooleanExpressions implements FlatJoinFunction<ArrayList<Long>, EdgeExtended<Long, Long, String, HashMap<String, String>>, ArrayList<Long>>{

	private FilterFunction<EdgeExtended<Long, Long, String, HashMap<String, String>>> filterEdges;
	
	
	public FilterInEdgesByBooleanExpressions(FilterFunction<EdgeExtended<Long, Long, String, HashMap<String, String>>> filterEdges) {
		this.filterEdges = filterEdges;
	}

	@Override
	public void join(
			ArrayList<Long> edgeId,
			EdgeExtended<Long, Long, String, HashMap<String, String>> edge,
			Collector<ArrayList<Long>> selectedVertexId) throws Exception {
		if(this.filterEdges.filter(edge) == true) {
			edgeId.add(edge.getEdgeId());
			edgeId.add(edge.getSourceId());
			selectedVertexId.collect(edgeId);
		}
	}
	
}

