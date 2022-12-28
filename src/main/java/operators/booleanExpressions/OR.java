package operators.booleanExpressions;

import operators.helper.FilterFunction;

public class OR <T> implements FilterFunction<T> {
	private FilterFunction<T> lhs, rhs;
  
	public OR(FilterFunction<T> l, FilterFunction<T> r) {
		this.lhs = l; 
		this.rhs = r;
    }

	@Override
	public boolean filter(T element) {
		return this.lhs.filter(element) || this.rhs.filter(element);
	}
  
}
