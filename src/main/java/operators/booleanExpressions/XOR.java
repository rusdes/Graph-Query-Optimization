package operators.booleanExpressions;
import operators.helper.FilterFunction;

public class XOR<T> implements FilterFunction<T> {
	private FilterFunction<T> lhs, rhs;
  
	public XOR(FilterFunction<T> l, FilterFunction<T> r) {
		this.lhs = l; 
		this.rhs = r;
    }

	@Override
	public boolean filter(T element) throws Exception {
		return (this.lhs.filter(element) && !this.rhs.filter(element)) || (!this.lhs.filter(element) && this.rhs.filter(element));
	}
  
}
