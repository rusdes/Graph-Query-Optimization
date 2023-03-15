import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;
import operators.helper.GraphCompressor;
import operators.helper.print_result;

import org.javatuples.*;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import queryplan.querygraph.QueryEdge;
import queryplan.querygraph.QueryGraph;
import queryplan.querygraph.QueryVertex;
import queryplan.*;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class CostBasedOptimizerTest {
	public static void main(String[] args) throws Exception {

		Boolean labeled = true; // change this to false to run on unlabeled data
		String dir = "src/test/java/Dataset";
		if (!labeled) {
			dir = "src/test/java/Dataset/unlabeled";
		}

		// defining source and target path for statistics files of edge and vertices
		String srcDir = dir;
		String tarDir = dir + "/Dataset_Statistics";
		String name_key= "Name";
	
		String testQuery = "0";
		Set<String> options = new HashSet<>();
		options.addAll(Arrays.asList("vertex_kdtree", "edges_kdtree"));
		Boolean compare = false;
		
		// Description for all options
		HashMap<String, ArrayList<String>> desc = new HashMap<>();
		desc.put("Initial Vertex Mapping Method", new ArrayList<>(Arrays.asList("vertex_naive", "vertex_kdtree")));
		desc.put("Edges Mapping Method", new ArrayList<>(Arrays.asList("edges_naive", "edges_kdtree")));

		// Write statistics to file if file is not present in tarDir
		StatisticsCollector stats= new StatisticsCollector(srcDir, tarDir);
		stats.collect();
		stats = null; // Free up memory
		
		List<Triplet<Long, String, String>> verticesFromFile = readVerticesLineByLine(Paths.get(dir, "vertices.csv"));
		List<Quintet<Long, Long, Long, String, String>> edgesFromFile = readEdgesLineByLine(
				Paths.get(dir, "edges.csv"));

		List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> vertices = verticesFromFile.stream()
				.map(elt -> VertexFromFileToDataSet(elt))
				.collect(Collectors.toList());
				
				List<EdgeExtended<Long, Long, String, HashMap<String, String>>> edges = edgesFromFile.stream()
				.map(elt -> EdgeFromFileToDataSet(elt))
				.collect(Collectors.toList());
				
				StatisticsTransformation sts = new StatisticsTransformation(tarDir);
				HashMap<String, Pair<Long, Double>> vstat = sts.getVerticesStatistics();
				HashMap<String, Pair<Long, Double>> estat = sts.getEdgesStatistics();
				GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph = GraphExtended
				.fromList(vertices, edges);

		// System.out.println(graph.getKDTreeByLabel("Artist").toString()); //check the
		// kd tree data
		GraphCompressor gc = new GraphCompressor();
		gc.compress(graph);
		QueryVertex[] vs = new QueryVertex[]{};
		QueryEdge[] es = new QueryEdge[]{};
		switch (testQuery) {
			case "0": {
				HashMap<String, Pair<String, String>> canelaCoxProps = new HashMap<>();
				canelaCoxProps.put("Name", new Pair<String, String>("eq", "Canela Cox"));
				QueryVertex a = new QueryVertex("Artist", canelaCoxProps, false);
				QueryVertex b = new QueryVertex("Band", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex c = new QueryVertex("Concert", new HashMap<String, Pair<String, String>>(), true);

				QueryEdge ab = new QueryEdge(a, b, "Part Of", new HashMap<String, Pair<String, String>>());
				QueryEdge bc = new QueryEdge(b, c, "Performed", new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[]{ a, b, c };
				es = new QueryEdge[]{ ab, bc };
				break;
			}

			case "16": {

				// find artists (who isnt canela cox) part of bands that performed in a concert
				// here result is not right, will throw error

				HashMap<String, Pair<String, String>> canelaCoxProps = new HashMap<>();
				canelaCoxProps.put("Years Active", new Pair<String, String>("<>", "Canela Cox"));
				QueryVertex a = new QueryVertex("Artist", canelaCoxProps, true);
				QueryVertex b = new QueryVertex("Band", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex c = new QueryVertex("Concert", new HashMap<String, Pair<String, String>>(), true);

				QueryEdge ab = new QueryEdge(a, b, "Part Of", new HashMap<String, Pair<String, String>>());
				QueryEdge bc = new QueryEdge(b, c, "Performed", new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[]{ a, b, c };
				es = new QueryEdge[]{ ab, bc };
				break;
			}

			case "17": {
				// HashMap<String, Pair<String, String>> canelaCoxProps = new HashMap<>();
				// canelaCoxProps.put("Name", new Pair<String, String>("eq", "Canela Cox"));
				QueryVertex a = new QueryVertex("Artist", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex b = new QueryVertex("Band", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex c = new QueryVertex("Concert", new HashMap<String, Pair<String, String>>(), true);

				QueryEdge ab = new QueryEdge(a, b, "Part Of", new HashMap<String, Pair<String, String>>());
				QueryEdge bc = new QueryEdge(a, c, "Performed", new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[]{ a, b, c };
				es = new QueryEdge[]{ ab, bc };
				break;
			}

			case "18": {
				// show all the artists that have performed in concerts
				QueryVertex a = new QueryVertex("Artist", new HashMap<String, Pair<String, String>>(), true);
				// QueryVertex b = new QueryVertex("Band", new HashMap<String, Pair<String,
				// String>>(), true);
				QueryVertex c = new QueryVertex("Concert", new HashMap<String, Pair<String, String>>(), true);

				// QueryEdge ab = new QueryEdge(a, b, "Part Of", new HashMap<String,
				// Pair<String, String>>());
				QueryEdge ac = new QueryEdge(a, c, "Performed", new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[]{ a, c };
				es = new QueryEdge[]{ ac };
				break;
			}

			case "19": {

				// search for concerts that started before 2020

				HashMap<String, Pair<String, String>> canelaCoxProps = new HashMap<>();
				canelaCoxProps.put("Started", new Pair<String, String>("<", "2020"));
				QueryVertex a = new QueryVertex("Concert", canelaCoxProps, true);
				// QueryVertex b = new QueryVertex("Band", new HashMap<String, Pair<String,
				// String>>(), true);
				// QueryVertex c = new QueryVertex("Concert", new HashMap<String, Pair<String,
				// String>>(), true);

				// QueryEdge ab = new QueryEdge(a, b, "Part Of", new HashMap<String,
				// Pair<String, String>>());
				// QueryEdge ac = new QueryEdge(a, c, "Performed", new HashMap<String,
				// Pair<String, String>>());

				vs = new QueryVertex[]{ a };
				es = new QueryEdge[]{};
				break;
			}

			case "20": {

				// search for artists and bands who performed in concerts after 2020 (OR case)

				HashMap<String, Pair<String, String>> concertProps = new HashMap<>();
				concertProps.put("Started", new Pair<String, String>(">", "2020"));
				QueryVertex a = new QueryVertex("Artist", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex b = new QueryVertex("Band", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex c = new QueryVertex("Concert", concertProps, true);
				// QueryVertex c = new QueryVertex("Concert", new HashMap<String, Pair<String,
				// String>>(), true);

				// QueryEdge ab = new QueryEdge(a, b, "Part Of", new HashMap<String,
				// Pair<String, String>>());
				QueryEdge ac = new QueryEdge(a, c, "Performed", new HashMap<String, Pair<String, String>>());
				QueryEdge bc = new QueryEdge(b, c, "Performed", new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[]{ a, b, c };
				es = new QueryEdge[]{ ac, bc };
				break;
			} 


			case "21" : {
				// IMDB query
				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
				// personProps.put("primaryName", new Pair<String, String>("=", "Harikrishnan Rajan"));

				HashMap<String, Pair<String, String>> movieProps = new HashMap<>();
				movieProps.put("originalTitle", new Pair<String, String>("eq", "Carmencita"));


				QueryVertex a = new QueryVertex("Person",  personProps, true);
				QueryVertex b = new QueryVertex("Movie",  movieProps, true);
				

				QueryEdge ab = new QueryEdge(a, b, "director", new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[]{a, b};
				es = new QueryEdge[]{ab};
				break;
			}

			case "22" : {
				// IMDB query
				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
				personProps.put("birthYear", new Pair<String, String>("<", "1899"));

				HashMap<String, Pair<String, String>> movieProps = new HashMap<>();
				// movieProps.put("originalTitle", new Pair<String, String>("eq", "Carmencita"));


				QueryVertex a = new QueryVertex("Person",  personProps, true);
				QueryVertex b = new QueryVertex("Movie",  movieProps, true);

				QueryEdge ab = new QueryEdge(a, b, "actor", new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[]{a, b};
				es = new QueryEdge[]{ab};
				break;
			}
		}

		QueryGraph g = new QueryGraph(vs, es);
		CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat, name_key);
		List<HashSet<HashSet<String>>> res = new ArrayList<>();
		List<HashSet<HashSet<String>>> res1 = new ArrayList<>();
		List<HashSet<HashSet<String>>> res2 = new ArrayList<>();
		List<HashSet<HashSet<String>>> res3 = new ArrayList<>();
		List<HashSet<HashSet<String>>> res4 = new ArrayList<>();

		System.out.println("Initialization Finished. \nStarting Query Execution...");
		if (compare) {
			System.out.println("for case "+ testQuery + ": \n");
			// Vertex Naive, Edge Naive
			// System.out.println("Vertex Naive, Edge Naive: ");
			// long startTimeNaive = System.nanoTime();
			// for (int i = 0; i < 1; i++) {
			// 	res1 = pg
			// 			.generateQueryPlan(new HashSet<>(Arrays.asList("vertex_naive", "edges_naive")));
			// }
			// long endTimeNaive = System.nanoTime();
			// System.out.println("time(ms): " + (endTimeNaive - startTimeNaive) / 1000000);
			// System.out.println("Results: " + res1);

			// Vertex Naive, Edge KDTree
			System.out.println("Vertex Naive, Edge KDtree: ");
			long startTimeVNaive = System.nanoTime();
			for (int i = 0; i < 1; i++) {
				res2 = pg.generateQueryPlan(
						new HashSet<>(Arrays.asList("vertex_naive", "edges_kdtree")));
			}
			long endTimeVNaive = System.nanoTime();
			System.out.println("time(ms): " + (endTimeVNaive - startTimeVNaive) / 1000000);
			System.out.println("Results: " + res2);

			// Vertex KDtree, Edge Naive
			System.out.println("Vertex KDtree, Edge Naive: ");
			long startTimeENaive = System.nanoTime();
			for (int i = 0; i < 1; i++) {
				res3 = pg.generateQueryPlan(
						new HashSet<>(Arrays.asList("vertex_kdtree", "edges_naive")));
			}
			long endTimeENaive = System.nanoTime();
			System.out.println("time(ms): " + (endTimeENaive - startTimeENaive) / 1000000);
			System.out.println("Results: " + res3);

			// Vertex KDtree, Edge KDtree
			System.out.println("Vertex KDtree, Edge KDtree");
			long startTimeKD = System.nanoTime();
			for (int i = 0; i < 1; i++) {
				res4 = pg.generateQueryPlan(
						new HashSet<>(Arrays.asList("vertex_kdtree", "edges_kdtree")));
			}
			long endTimeKD = System.nanoTime();
			System.out.println("time(ms): " + (endTimeKD - startTimeKD) / 1000000);
			System.out.println("Results: " + res4);
		} else {
			System.out.println(options);
			res = pg.generateQueryPlan(options);
			System.out.print(res);
			

			print_result obj= new print_result(graph, res);
			obj.printTable();
		}
	}



	public static List<Triplet<Long, String, String>> readVerticesLineByLine(Path filePath) throws Exception {
		List<Triplet<Long, String, String>> list = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(filePath)) {
			try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1)
					.withCSVParser(new CSVParserBuilder().withSeparator('|').build())
					.build()) {
				String[] line;
				while ((line = csvReader.readNext()) != null) {
					Triplet<Long, String, String> holder = new Triplet<Long, String, String>(Long.parseLong(line[0]),
							line[1], line[2]);
					list.add(holder);
				}
			}
		}
		return list;
	}

	public static List<Quintet<Long, Long, Long, String, String>> readEdgesLineByLine(Path filePath) throws Exception {
		List<Quintet<Long, Long, Long, String, String>> list = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(filePath)) {
			try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1)
					.withCSVParser(new CSVParserBuilder().withSeparator('|').build())
					.build()) {
				String[] line;
				while ((line = csvReader.readNext()) != null) {
					Quintet<Long, Long, Long, String, String> holder = new Quintet<Long, Long, Long, String, String>(
							Long.parseLong(line[0]), Long.parseLong(line[1]),
							Long.parseLong(line[2]), line[3], line[4]);
					list.add(holder);
				}
			}
		}
		return list;
	}

	public static VertexExtended<Long, HashSet<String>, HashMap<String, String>> VertexFromFileToDataSet(
			Triplet<Long, String, String> vertexFromFile) {
		VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex = new VertexExtended<Long, HashSet<String>, HashMap<String, String>>();
		vertex.setVertexId(vertexFromFile.getValue0());

		String label = vertexFromFile.getValue1().split(",")[0];
		vertex.setLabel(label);

		HashMap<String, String> properties = new HashMap<>();
		String[] props = vertexFromFile.getValue2().split(",");

		if (props.length > 1) {
			for (int i = 0; i < props.length - 1; i = i + 2) {
				properties.put(props[i], props[i + 1]);
			}
		}
		vertex.setProps(properties);

		return vertex;
	}

	public static EdgeExtended<Long, Long, String, HashMap<String, String>> EdgeFromFileToDataSet(
			Quintet<Long, Long, Long, String, String> edgeFromFile) {

		EdgeExtended<Long, Long, String, HashMap<String, String>> edge = new EdgeExtended<Long, Long, String, HashMap<String, String>>();

		edge.setEdgeId(edgeFromFile.getValue0());
		edge.setSourceId(edgeFromFile.getValue1());
		edge.setTargetId(edgeFromFile.getValue2());
		edge.setLabel(edgeFromFile.getValue3());

		HashMap<String, String> properties = new HashMap<>();
		String[] props = edgeFromFile.getValue4().split(",");
		if (props.length > 1) {
			for (int i = 0; i < props.length - 1; i = i + 2) {
				properties.put(props[i], props[i + 1]);
			}
		}
		edge.setProps(properties);

		return edge;

	}
}
