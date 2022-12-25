package ldbc.map;

import java.util.HashMap;
import java.util.HashSet;

import operators.datastructures.EdgeExtended;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Pair;
import org.apache.flink.api.java.tuple.Quartet;

@SuppressWarnings("serial")
public class HasCreatorMap implements CrossFunction<Pair<Long, Long>, 
		Quartet<Long, HashSet<String>, HashMap<String, String>, Long>, 
		EdgeExtended<Long, Long, String, HashMap<String, String>>> {
	private long newId = 1L;
	private String label; 
	
	public HasCreatorMap(String label) {this.label = label;}
	@Override
	public EdgeExtended<Long, Long, String, HashMap<String, String>> cross(
			Pair<Long, Long> vertexIdsOfEdge,
			Quartet<Long, HashSet<String>, HashMap<String, String>, Long> maxId)
			throws Exception {
		
		EdgeExtended<Long, Long, String, HashMap<String, String>> edge = new EdgeExtended<>();
		edge.setEdgeId(newId + maxId.f0);
		this.newId ++;
		
		edge.setSourceId(vertexIdsOfEdge.f0);
		edge.setTargetId(vertexIdsOfEdge.f1);
		
		edge.setLabel(label);

		HashMap<String, String> props = new HashMap<>();
		edge.setProps(props);
		
		return edge;
	}
	
	
}
