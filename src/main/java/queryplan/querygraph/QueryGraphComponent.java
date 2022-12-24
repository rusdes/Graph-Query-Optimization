package queryplan.querygraph;
import java.util.ArrayList;
import java.util.List;

// import org.apache.flink.api.java.List;
public class QueryGraphComponent {
	//indicate the estimated cardinality of the current graph component
	double est;

	//indicate the positions of all retrieved results in returned paths
	List<ArrayList<Long>> data;

	//store vertices and edges are contained in this graph component
	ArrayList<Object> columns;
	
	
	public QueryGraphComponent(double e, List<ArrayList<Long>> d, ArrayList<Object> cols) {
		est = e;
		data = d;
		columns = cols;
	}
	
	public double getEst() {
		return est;
	} 
	
	public List<ArrayList<Long>> getData() {
		return data;
	}
	
	public ArrayList<Object> getColumns() {
		return columns;
	}
	
	public int getVertexIndex(QueryVertex qv) {
		for(int i = 0; i < columns.size(); i++) {
			if(columns.get(i) == qv) return i;
		}
		return -1;
	}
}