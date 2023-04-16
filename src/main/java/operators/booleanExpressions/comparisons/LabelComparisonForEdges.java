package operators.booleanExpressions.comparisons;

import java.util.HashMap;

import operators.datastructures.EdgeExtended;
import operators.helper.FilterFunction;

public class LabelComparisonForEdges implements FilterFunction<EdgeExtended>{

	private String label;
	
	public LabelComparisonForEdges(String label) {this.label = label;}
	@Override
	public boolean filter(
			EdgeExtended edge)
			throws Exception {
		if(edge.getLabel().equals(this.label) || label.equals("")) return true;
		else return false;
	}

}
