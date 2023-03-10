package operators.booleanExpressions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import operators.datastructures.EdgeExtended;
import operators.datastructures.VertexExtended;
import operators.helper.FilterFunction;

// import org.apache.flink.api.common.functions.FlatJoinFunction;
// import operators.helper.FlatJoinFunction;
// import org.apache.flink.util.Collector;
// import operators.helper.Collector;;

// @SuppressWarnings("serial")
public class FilterOutEdgesByBooleanExpressions{

	private FilterFunction<EdgeExtended<Long, Long, String, HashMap<String, String>>> filterEdges;
	private FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> filterVertices;
	
	public FilterOutEdgesByBooleanExpressions(FilterFunction<EdgeExtended<Long, Long, String, HashMap<String, String>>> filterEdges,
										FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> filterVertices) {
		this.filterEdges = filterEdges;
		this.filterVertices = filterVertices;
	}

	// @Override
	public boolean join(
			EdgeExtended<Long, Long, String, HashMap<String, String>> edge,
			VertexExtended<Long, HashSet<String>, HashMap<String, String>> nextVertex
			) throws Exception {
		if(this.filterEdges.filter(edge) == true && this.filterVertices.filter(nextVertex) == true){
			return true;
		}
		return false;
	}
	
}

