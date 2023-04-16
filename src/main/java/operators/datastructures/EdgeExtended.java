package operators.datastructures;

import java.util.HashMap;

// import org.apache.flink.api.java.tuple.Quintet;
// import org.javatuples.Quintet;
/**
 * Extended edge for Cypher Implementation
 * @param <E> the key type for the edges
 * @param <K> the key type for the sources and target vertices
 * @param <L> the edge label type
 * @param <P> the edge properties type
 *  */

public class EdgeExtended implements java.io.Serializable{
	
	// private static final long serialVersionUID = 1L;
	// Long, Long, String, HashMap<String, String>
	Long f0;
	Long f1, f2;
	String f3;
	HashMap<String, String> f4;
	public EdgeExtended(){}

	public EdgeExtended(Long edgeId, Long srcId, Long trgId, String label, HashMap<String, String> props) {
		this.f0 = edgeId;
		this.f1 = srcId;
		this.f2 = trgId;
		this.f3 = label;
		this.f4 = props;
	}
	
	public void setEdgeId(Long edgeId) {
		this.f0 = edgeId;
	}

	public Long getEdgeId() {
		return this.f0;
	}
	
	public void setSourceId(Long srcId) {
		this.f1 = srcId;
	}

	public Long getSourceId() {
		return this.f1;
	}

	public void setTargetId(Long targetId) {
		this.f2 = targetId;
	}

	public Long getTargetId() {
		return f2;
	}

	public void setLabel(String label) {
		this.f3 = label;
	}

	public String getLabel() {
		return f3;
	}

	public void setProps(HashMap<String, String> props) {
		this.f4 = props;
	}	
	
	public HashMap<String, String> getProps() {
		return f4;
	}
		
}
