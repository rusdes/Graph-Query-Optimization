package operators.booleanExpressions.comparisons;

import java.util.HashMap;
import java.util.HashSet;

import operators.datastructures.VertexExtended;
import operators.helper.FilterFunction;

public class PropertyFilterForVertices
		implements FilterFunction<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> {

	private String propertyKey;
	private String op;
	private String propertyValue;
	private double propertyValueDouble;

	public PropertyFilterForVertices(String propertyKey, String op, String propertyValue) {
		this.propertyKey = propertyKey;
		this.op = op;
		this.propertyValue = propertyValue;
		try {
			this.propertyValueDouble = Double.parseDouble(propertyValue);
		} catch (Exception e) {
			this.propertyValueDouble = 0;
		}
	}

	@Override
	public boolean filter(VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex) throws Exception {
		String vp = vertex.getProps().get(this.propertyKey);

		if (vp.equals("unknown")) { // == null originally. Changed to unknown
			return false; 
		}
		
		Double vp_num;
		try {
			vp_num = Double.parseDouble(vp);
		} catch (Exception e) {
			// e.printStackTrace();
			vp_num = 0.0;
		}

		switch (op) {
			case ">": {
				return vp_num > this.propertyValueDouble;
			}
			case "<": {
				return vp_num < this.propertyValueDouble;
			}
			case "=": {
				return vp_num == this.propertyValueDouble;
			}
			case ">=": {
				return vp_num >= this.propertyValueDouble;
			}
			case "<=": {
				return vp_num <= this.propertyValueDouble;
			}
			case "<>": {
				Boolean a;
				try{
					// try as double
					a = (vp_num != this.propertyValueDouble);
				}catch (Exception e){
					// try as string
					a = !vertex.getProps().get(this.propertyKey).equals(this.propertyValue);
				}
				return a;
			}
			case "eq":{
				return vp.equals(this.propertyValue);
			}
			default:
				throw new Exception("Bad operator " + op + " !");
		}
	}
}
