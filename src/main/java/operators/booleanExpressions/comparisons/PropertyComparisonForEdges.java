package operators.booleanExpressions.comparisons;

import java.util.ArrayList;
import java.util.HashMap;

import operators.datastructures.EdgeExtended;
import operators.helper.Collector;
// import org.apache.flink.api.common.functions.FlatJoinFunction;
import operators.helper.FlatJoinFunction;
// import org.apache.flink.util.Collector;

public class PropertyComparisonForEdges implements
		FlatJoinFunction<ArrayList<Long>, EdgeExtended<Long, Long, String, HashMap<String, String>>, ArrayList<Long>> {

	private String propertyKey;
	private String op;
	private double propertyValue;

	public PropertyComparisonForEdges(String propertyKey, String op, double propertyValue) {
		this.propertyKey = propertyKey;
		this.op = op;
		this.propertyValue = propertyValue;
	}

	@Override
	public void join(ArrayList<Long> edgeId,
			EdgeExtended<Long, Long, String, HashMap<String, String>> edge,
			Collector<ArrayList<Long>> selectedEdgeId) throws Exception {
		if (edge.getProps().get(this.propertyKey) == null) {
			return;
		} else {
			switch (op) {
				case ">": {
					if (Double.parseDouble(edge.getProps().get(this.propertyKey)) > this.propertyValue)
						selectedEdgeId.collect(edgeId);
					else
						return;
				}
				case "<": {
					if (Double.parseDouble(edge.getProps().get(this.propertyKey)) < this.propertyValue)
						selectedEdgeId.collect(edgeId);
					else
						return;
				}
				case "=": {
					if (Double.parseDouble(edge.getProps().get(this.propertyKey)) == this.propertyValue)
						selectedEdgeId.collect(edgeId);
					else
						return;
				}
				case ">=": {
					if (Double.parseDouble(edge.getProps().get(this.propertyKey)) >= this.propertyValue)
						selectedEdgeId.collect(edgeId);
					else
						return;
				}
				case "<=": {
					if (Double.parseDouble(edge.getProps().get(this.propertyKey)) <= this.propertyValue)
						selectedEdgeId.collect(edgeId);
					else
						return;
				}
				case "<>": {
					if (Double.parseDouble(edge.getProps().get(this.propertyKey)) != this.propertyValue)
						selectedEdgeId.collect(edgeId);
					else
						return;
				}
				default:
					return;
			}
		}
	}
}
