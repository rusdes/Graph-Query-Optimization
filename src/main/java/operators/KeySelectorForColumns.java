package operators;
import java.util.List;


/*
 * key selector is used for determining the position of a vertex or an edge in a path ArrayList
 * 
 * */
public class KeySelectorForColumns {
	
	private int col = 0;
	
	KeySelectorForColumns(int column) {this.col = column;}

	public Long getKey(List<Long> row) {
		return row.get(col);
	}
}

