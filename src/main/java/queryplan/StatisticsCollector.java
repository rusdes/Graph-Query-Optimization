package queryplan;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
// import org.apache.flink.api.java.DataSet;
// import org.apache.flink.api.java.tuple.Pair;
import org.javatuples.*;
// import org.apache.flink.api.java.tuple.Triplet;
// import org.apache.flink.api.java.tuple.Quintet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
/**
 * Collect statistic information from graph database
 * Information includes the total number of edges and vertices,
 * vertex and edge labels and their corresponding proportion of the total number
 * */
@SuppressWarnings("serial")
public class StatisticsCollector {
	public static void main(String[] args) throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		String srcDir = args[0];
		String tarDir = args[1];
		StatisticsCollector k = new StatisticsCollector(env, srcDir, tarDir);
		k.getEdgesStats();
		k.getVerticesStats();
	}	
	ExecutionEnvironment env;
	String srcDir;
	String tarDir;
	
	public StatisticsCollector(ExecutionEnvironment e, String s, String t) {
		env = e;
		srcDir = s;
		tarDir = t;
	}

	//This function will write a three-column file to the target directory. The three column consists of the vertex label names, the number of the vertices and the proportion.
	public Triplet<String, Long, Double>  getVerticesStats () throws Exception {
		
		Triplet<Long, String, String> verticesFromFile = env.readCsvFile(srcDir + "vertices.csv")
			.fieldDelimiter("|")
			.types(Long.class, String.class, String.class);
		
		Pair<String, Long> vertexLabels = verticesFromFile.flatMap(new ExtractVertexLabels()).groupBy(0).sum(1); 
		
		long vertexNum = verticesFromFile.count();
		
		Pair<String, Long> vertexNumber = env.fromElements("vertices" + " " + String.valueOf(vertexNum)).map(new StringToTuple());
		
		Triplet<String, Long, Double> vertices = vertexLabels.union(vertexNumber)
				.map(new ProportionComputation(vertexNum))
				.sortPartition(1, Order.DESCENDING);
		
		vertices.writeAsCsv(tarDir + "vertices", "\n", "|");
		
		env.execute();
		return vertices;
	}

	//This function will write a three-column file to the target directory. The three column consists of the edge label names, the number of the vertices and the proportion.
	public Triplet<String, Long, Double> getEdgesStats () throws Exception {
		
		Quintet<Long, Long, Long, String, String> edgesFromFile = env.readCsvFile(srcDir + "edges.csv")
				.fieldDelimiter("|")
				.types(Long.class, Long.class, Long.class, String.class, String.class);
		
		Pair<String, Long> edgeLabels = edgesFromFile.map(new ExtractEdgeLabels()).groupBy(0).sum(1); 
		
		long edgeNum = edgesFromFile.count();
		
		Pair<String, Long> edgeNumber = env.fromElements("edges" + " " + String.valueOf(edgeNum)).map(new StringToTuple());
		
		Triplet<String, Long, Double> edges = edgeLabels.union(edgeNumber)
				.map(new ProportionComputation(edgeNum))
				.sortPartition(1, Order.DESCENDING);
		

		edges.writeAsCsv(tarDir + "edges", "\n", "|");
		env.execute();
		return edges;

	}
	

	private static class ExtractVertexLabels implements FlatMapFunction<Triplet<Long, String, String>, Pair<String, Long>>{

		@Override
		public void flatMap(Triplet<Long, String, String> vertex, Collector<Pair<String, Long>> labels)
				throws Exception {
			String[] ls = vertex.f1.substring(1, vertex.f1.length()-1).split(",");
			for(String label : ls) {
				labels.collect(new Pair<String, Long>(label, 1L));
			}
		}
	}

	private static class StringToTuple implements MapFunction<String, Pair<String, Long>> {

		@Override
		public Pair<String, Long> map(String sum) throws Exception {
			String[] total = sum.split(" ");
			return new Pair<String, Long>(total[0], Long.valueOf(total[1]));
		}
	}
	
	private static class ProportionComputation implements MapFunction<Pair<String, Long>, Triplet<String, Long, Double>> {

		private long totalNum;
		public ProportionComputation(long totalNum) { this.totalNum = totalNum; }
		
		@Override
		public Triplet<String, Long, Double> map(Pair<String, Long> vertex)
				throws Exception {
			return new Triplet<String, Long, Double>(vertex.f0, vertex.f1, (double)vertex.f1 * 1.0/totalNum);
		}
	}
	
	private static class ExtractEdgeLabels implements MapFunction<Quintet<Long, Long, Long, String, String>, Pair<String, Long>> {

		@Override
		public Pair<String, Long> map(
				Quintet<Long, Long, Long, String, String> edge)
				throws Exception {
			return new Pair<String, Long>(edge.f3, 1L);
		}
	}
}
