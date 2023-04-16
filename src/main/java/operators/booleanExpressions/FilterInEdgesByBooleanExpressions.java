package operators.booleanExpressions;

import java.util.ArrayList;
import java.util.HashMap;

import operators.datastructures.EdgeExtended;
import operators.helper.Collector;
import operators.helper.FilterFunction;

// import org.apache.flink.api.common.functions.FlatJoinFunction;
import operators.helper.FlatJoinFunction;
// import org.apache.flink.util.Collector;

public class FilterInEdgesByBooleanExpressions implements FlatJoinFunction<ArrayList<Long>, EdgeExtended, ArrayList<Long>>{

	private FilterFunction<EdgeExtended> filterEdges;
	
	
	public FilterInEdgesByBooleanExpressions(FilterFunction<EdgeExtended> filterEdges) {
		this.filterEdges = filterEdges;
	}

	@Override
	public void join(
			ArrayList<Long> edgeId,
			EdgeExtended edge,
			Collector<ArrayList<Long>> selectedVertexId) throws Exception {
		if(this.filterEdges.filter(edge) == true) {
			edgeId.add(edge.getEdgeId());
			edgeId.add(edge.getSourceId());
			selectedVertexId.collect(edgeId);
		}
	}
	
}

