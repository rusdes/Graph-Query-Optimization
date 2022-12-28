package operators.booleanExpressions;

import operators.helper.FilterFunction;

public class NOT<T> implements FilterFunction<T> {
	
	private FilterFunction<T> ft;

	public NOT(FilterFunction<T> ft) {this.ft = ft;}
	
	@Override
	public boolean filter(T element) {
		return !ft.filter(element);
	}	
}
