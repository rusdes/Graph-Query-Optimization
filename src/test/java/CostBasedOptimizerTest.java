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

import java.io.File;
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

		// Boolean labeled = true; // change this to false to run on unlabeled data
		// System.out.println(
		// "1 - Compressed IMDB\n2 - Uncompressed IMDB\n3 - Unlabeled Toy Dataset\n4 -
		// Labeled Toy Dataset");
		int choice = 2;
		String testQuery = "30";
		System.out.println("Choice: " + choice);

		String dir = null;
		String name_key = null;
		Boolean compare = false;

		Set<String> options = new HashSet<>();
		options.addAll(Arrays.asList("vertex_naive", "edges_kdtree"));

		// Description for all options
		HashMap<String, ArrayList<String>> desc = new HashMap<>();
		desc.put("Initial Vertex Mapping Method", new ArrayList<>(Arrays.asList("vertex_naive", "vertex_kdtree")));
		desc.put("Edges Mapping Method", new ArrayList<>(Arrays.asList("edges_naive", "edges_kdtree")));

		switch (choice) {
			case 1: {
				// Query- 21, 22, 23, 24
				// dir = "src/test/java/Dataset/compressed_imdb";
				dir = "src/test/java/Dataset/IMDB_Small";
				name_key = "name";
				// testQuery = "23";
				// testQuery = "22";
				break;
			}

			case 2: {
				// Query- 30,
				dir = "src/test/java/Dataset/uncompressed_imdb";
				// testQuery = "30";
				name_key = "value";
				testQuery = "31";
				// testQuery = "32";
				break;
			}

			case 3: {
				// unlabelled dataset
				dir = "src/test/java/Dataset/unlabeled";
				// testQuery = null;
				break;
			}

			case 4: {
				// toy dataset
				// Query: 40, 41, 42, 43, 44, 45
				dir = "src/test/java/Dataset/";
				name_key = "Name";
				// testQuery = "0";
				// testQuery = "16";
				// testQuery = "0";
				// testQuery = "18";
				// testQuery = "19";
				// testQuery = "45";
				break;
			}

			case 5: {
				// Query: 40, 41, 42, 43, 44, 45
				dir = "src/test/java/Dataset/compressed_imdb";
				name_key = "primaryName";
				// testQuery = "0";
				// testQuery = "16";
				// testQuery = "0";
				// testQuery = "18";
				// testQuery = "19";
				// testQuery = "45";
				break;
			}

			case 6: {
				// Query- 30,
				dir = "src/test/java/Dataset/IMDB_Medium";
				// testQuery = "30";
				name_key = "name";
				// testQuery = "31";
				// testQuery = "32";
				break;
			}

			case 7: {
				// Query- 30,
				dir = "src/test/java/Dataset/IMDB_Large";
				// testQuery = "30";
				name_key = "name";
				// testQuery = "31";
				// testQuery = "32";
				break;
			}

			case 8: {
				// Query- 30,
				dir = "src/test/java/Dataset/exp_data";
				// testQuery = "30";
				name_key = "value";
				// testQuery = "31";
				testQuery = "32";
				break;
			}
		}

		// defining source and target path for statistics files of edge and vertices
		String srcDir = dir;
		String tarDir = dir + "/Dataset_Statistics";
		File theDir = new File(tarDir);
		if (!theDir.exists()) {
			theDir.mkdirs();
		}

		// Write statistics to file if file is not present in tarDir
		StatisticsCollector stats = new StatisticsCollector(srcDir, tarDir);
		stats.collect();
		stats = null; // Free up memory

		List<Triplet<Long, String, String>> verticesFromFile = readVerticesLineByLine(Paths.get(dir, "vertices.csv"));
		List<Quintet<Long, Long, Long, String, String>> edgesFromFile = readEdgesLineByLine(
				Paths.get(dir, "edges.csv"));

		List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> vertices = verticesFromFile.stream()
				.map(elt -> VertexFromFileToDataSet(elt)).collect(Collectors.toList());

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
		// GraphCompressor gc = new GraphCompressor();
		// gc.compress(graph);
		QueryVertex[] vs = new QueryVertex[] {};
		QueryEdge[] es = new QueryEdge[] {};
		switch (testQuery) {

			case "40": {
				// expected output: [[4, 5], [6, 5]]
				HashMap<String, Pair<String, String>> canelaCoxProps = new HashMap<>();
				canelaCoxProps.put("Name", new Pair<String, String>("eq", "Canela Cox"));
				QueryVertex a = new QueryVertex("Artist", canelaCoxProps, false);
				QueryVertex b = new QueryVertex("Band", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex c = new QueryVertex("Concert", new HashMap<String, Pair<String, String>>(), true);

				QueryEdge ab = new QueryEdge(a, b, "Part Of", new HashMap<String, Pair<String, String>>());
				QueryEdge bc = new QueryEdge(b, c, "Performed", new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[] { a, b, c };
				es = new QueryEdge[] { ab, bc };
				break;
			}

			case "41": {

				// expected output: [[4, 2, 5]]

				HashMap<String, Pair<String, String>> canelaCoxProps = new HashMap<>();
				canelaCoxProps.put("Years Active", new Pair<String, String>("<=", "10"));
				QueryVertex a = new QueryVertex("Artist", canelaCoxProps, true);
				QueryVertex b = new QueryVertex("Band", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex c = new QueryVertex("Concert", new HashMap<String, Pair<String, String>>(), true);

				QueryEdge ab = new QueryEdge(a, b, "Part Of", new HashMap<String, Pair<String, String>>());
				QueryEdge bc = new QueryEdge(b, c, "Performed", new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[] { a, b, c };
				es = new QueryEdge[] { ab, bc };
				break;
			}

			case "42": {
				// HashMap<String, Pair<String, String>> canelaCoxProps = new HashMap<>();
				// canelaCoxProps.put("Name", new Pair<String, String>("eq", "Canela Cox"));

				// expected output: [[4, 3, 5], [4, 3, 7], [4, 3, 8], [4, 2, 5], [6, 3, 5], [6,
				// 3, 7], [6, 3, 8]]
				QueryVertex a = new QueryVertex("Artist", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex b = new QueryVertex("Band", new HashMap<String, Pair<String, String>>(), true);
				QueryVertex c = new QueryVertex("Concert", new HashMap<String, Pair<String, String>>(), true);

				QueryEdge ab = new QueryEdge(a, b, "Part Of", new HashMap<String, Pair<String, String>>());
				QueryEdge ac = new QueryEdge(a, c, "Performed", new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[] { a, b, c };
				es = new QueryEdge[] { ab, ac };
				break;
			}

			case "43": {
				// show all the artists that have performed in concerts

				// expected output: [[2, 5], [3, 5], [3, 7], [3, 8]]
				QueryVertex a = new QueryVertex("Artist", new HashMap<String, Pair<String, String>>(), true);
				// QueryVertex b = new QueryVertex("Band", new HashMap<String, Pair<String,
				// String>>(), true);
				QueryVertex c = new QueryVertex("Concert", new HashMap<String, Pair<String, String>>(), true);

				// QueryEdge ab = new QueryEdge(a, b, "Part Of", new HashMap<String,
				// Pair<String, String>>());
				QueryEdge ac = new QueryEdge(a, c, "Performed", new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[] { a, c };
				es = new QueryEdge[] { ac };
				break;
			}

			case "44": {

				// search for concerts that started before 2020

				// expected output: [[8]]

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

				vs = new QueryVertex[] { a };
				es = new QueryEdge[] {};
				break;
			}

			case "45": {

				// search for artists and bands who performed in concerts after 2020 (OR case)
				// expected output- [[4, 5, 3], [4, 5, 2], [6, 5, 3], [6, 5, 2]]
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

				vs = new QueryVertex[] { a, b, c };
				es = new QueryEdge[] { ac, bc };
				break;
			}

			case "21": {
				// IMDB query
				// expected output: [[1, 105666]]
				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
				// personProps.put("primaryName", new Pair<String, String>("=", "Harikrishnan
				// Rajan"));

				HashMap<String, Pair<String, String>> movieProps = new HashMap<>();
				movieProps.put("originalTitle", new Pair<String, String>("eq", "Carmencita"));

				QueryVertex a = new QueryVertex("Person", personProps, true);
				QueryVertex b = new QueryVertex("Movie", movieProps, true);

				QueryEdge ab = new QueryEdge(a, b, "director", new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[] { a, b };
				es = new QueryEdge[] { ab };
				break;
			}

			case "22": {
				// IMDB query
				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
				personProps.put("birthYear", new Pair<String, String>("<", "1810"));

				HashMap<String, Pair<String, String>> movieProps = new HashMap<>();
				// movieProps.put("originalTitle", new Pair<String, String>("eq",
				// "Carmencita"));

				QueryVertex a = new QueryVertex("Person", personProps, true);
				QueryVertex b = new QueryVertex("Movie", movieProps, false);

				QueryEdge ab = new QueryEdge(a, b, "composer", new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[] { a, b };
				es = new QueryEdge[] { ab };
				break;
			}

			case "23": {
				// IMDB query
				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
				personProps.put("primaryProfession", new Pair<String, String>("eq", "camera_department"));

				HashMap<String, Pair<String, String>> movieProps = new HashMap<>();
				// movieProps.put("originalTitle", new Pair<String, String>("eq",
				// "Carmencita"));

				QueryVertex a = new QueryVertex("Person", personProps, true);
				QueryVertex b = new QueryVertex("Movie", movieProps, false);

				QueryEdge ab = new QueryEdge(a, b, "director", new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[] { a, b };
				es = new QueryEdge[] { ab };
				break;
			}

			case "24": {
				// IMDB query
				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
				// personProps.put("primaryProfession", new Pair<String, String>("eq",
				// "actor"));

				HashMap<String, Pair<String, String>> movieProps = new HashMap<>();
				// movieProps.put("originalTitle", new Pair<String, String>("eq",
				// "Carmencita"));

				QueryVertex a = new QueryVertex("Person", personProps, true);
				QueryVertex b = new QueryVertex("Movie", movieProps, true);

				QueryEdge ab = new QueryEdge(a, b, "archive_footage", new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[] { a, b };
				es = new QueryEdge[] { ab };
				break;
			}

			case "25": {
				// IMDB query
				// dont run for now, gotta test this query
				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
				// personProps.put("primaryProfession", new Pair<String, String>("eq",
				// "camera_department"));

				HashMap<String, Pair<String, String>> movieProps = new HashMap<>();
				// movieProps.put("originalTitle", new Pair<String, String>("eq",
				// "Carmencita"));

				QueryVertex a = new QueryVertex("Person", personProps, true);
				QueryVertex b = new QueryVertex("Movie", movieProps, true);
				QueryVertex c = new QueryVertex("Movie", movieProps, true);

				QueryEdge ab = new QueryEdge(a, b, "director", new HashMap<String, Pair<String, String>>());
				QueryEdge ac = new QueryEdge(a, c, "director", new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[] { a, b };
				es = new QueryEdge[] { ab, ac };
				break;
			}

			case "30": {
				// IMDB uncompressed
				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
				HashMap<String, Pair<String, String>> birthYearProps = new HashMap<>();
				birthYearProps.put("value", new Pair<String, String>("<", "1810"));
				HashMap<String, Pair<String, String>> movieProps = new HashMap<>();
				// movieProps.put("originalTitle", new Pair<String, String>("eq",
				// "Carmencita"));

				QueryVertex a = new QueryVertex("Person", personProps, true);
				QueryVertex b = new QueryVertex("birthYear", birthYearProps, true);
				QueryVertex c = new QueryVertex("Movie", movieProps, false);

				QueryEdge ac = new QueryEdge(a, c, "composer",
						new HashMap<String, Pair<String, String>>());
				QueryEdge ab = new QueryEdge(a, b, "hasProperty",
						new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[] { a, b, c };
				es = new QueryEdge[] { ac, ab };
				break;
			}

			case "31": {
				// IMDB uncompressed
				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
				HashMap<String, Pair<String, String>> birthYearProps = new HashMap<>();
				birthYearProps.put("value", new Pair<String, String>("<", "1810"));
				// HashMap<String, Pair<String, String>> movieProps = new HashMap<>();
				// // movieProps.put("originalTitle", new Pair<String, String>("eq",
				// "Carmencita"));

				QueryVertex a = new QueryVertex("Person", personProps, true);
				QueryVertex b = new QueryVertex("birthYear", birthYearProps, true);
				// QueryVertex c = new QueryVertex("Movie", movieProps, false);

				// QueryEdge ac = new QueryEdge(a, c, "composer",
				// new HashMap<String, Pair<String, String>>());
				QueryEdge ab = new QueryEdge(a, b, "hasProperty",
						new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[] { a, b, };
				es = new QueryEdge[] { ab };
				break;
			}

			case "32": {
				// IMDB uncompressed
				HashMap<String, Pair<String, String>> personProps = new HashMap<>();
				HashMap<String, Pair<String, String>> birthYearProps = new HashMap<>();
				birthYearProps.put("value", new Pair<String, String>("eq", "Danny Acosta"));
				// HashMap<String, Pair<String, String>> movieProps = new HashMap<>();
				// // movieProps.put("originalTitle", new Pair<String, String>("eq",
				// "Carmencita"));

				QueryVertex a = new QueryVertex("Person", personProps, true);
				QueryVertex b = new QueryVertex("primaryName", birthYearProps, true);
				// QueryVertex c = new QueryVertex("Movie", movieProps, false);

				// QueryEdge ac = new QueryEdge(a, c, "composer",
				// new HashMap<String, Pair<String, String>>());
				QueryEdge ab = new QueryEdge(a, b, "hasProperty",
						new HashMap<String, Pair<String, String>>());

				vs = new QueryVertex[] { a, b, };
				es = new QueryEdge[] { ab };
				break;
			}

			// case "30": {
			// // IMDB uncompressed
			// HashMap<String, Pair<String, String>> personProps = new HashMap<>();
			// HashMap<String, Pair<String, String>> birthYearProps = new HashMap<>();
			// birthYearProps.put("value", new Pair<String, String>("<", "1850"));
			// HashMap<String, Pair<String, String>> movieProps = new HashMap<>();
			// // movieProps.put("originalTitle", new Pair<String, String>("eq",
			// // "Carmencita"));

			// QueryVertex person = new QueryVertex("Person", personProps, true);
			// QueryVertex birthYear = new QueryVertex("birthYear", birthYearProps, true);
			// QueryVertex movie = new QueryVertex("Movie", movieProps, false);

			// QueryEdge personMovie = new QueryEdge(person, movie, "actor",
			// new HashMap<String, Pair<String, String>>());
			// QueryEdge personBirthYear = new QueryEdge(person, birthYear, "hasProperty",
			// new HashMap<String, Pair<String, String>>());

			// vs = new QueryVertex[] { person, birthYear, movie };
			// es = new QueryEdge[] { personMovie, personBirthYear };
			// break;
			// }
		}

		QueryGraph g = new QueryGraph(vs, es);
		// CostBasedOptimzerNew pg = new CostBasedOptimzerNew(g, graph, vstat, estat,
		// name_key);
		CostBasedOptimzer pg = new CostBasedOptimzer(g, graph, vstat, estat);

		// List<HashSet<Set<String>>> res = new ArrayList<>();
		// List<HashSet<Set<String>>> res1 = new ArrayList<>();
		// List<HashSet<Set<String>>> res2 = new ArrayList<>();
		// List<HashSet<Set<String>>> res3 = new ArrayList<>();
		// List<HashSet<Set<String>>> res4 = new ArrayList<>();

		List<List<Long>> res = new ArrayList<>();
		List<List<Long>> res1 = new ArrayList<>();
		List<List<Long>> res2 = new ArrayList<>();
		List<List<Long>> res3 = new ArrayList<>();
		List<List<Long>> res4 = new ArrayList<>();

		// Garbage Collector
		verticesFromFile = null;
		edgesFromFile = null;
		vertices = null;
		edges = null;
		vs = null;
		es = null;
		g = null;
		vstat = null;
		estat = null;

		System.out.println("Initialization Finished. \nStarting Query Execution...");
		if (compare) {
			System.out.println("for case " + testQuery + ": \n");

			// Vertex Naive, Edge Naive
			System.out.println("Vertex Naive, Edge Naive: ");
			long startTimeNaive = System.nanoTime();
			for (int i = 0; i < 1; i++) {
				res1 = pg
						.generateQueryPlan(new HashSet<>(Arrays.asList("vertex_naive",
								"edges_naive")));
			}
			long endTimeNaive = System.nanoTime();
			System.out.println("time(ms): " + (endTimeNaive - startTimeNaive) / 1000000);
			System.out.println("Results: " + res1);

			// Vertex Naive, Edge KDTree
			System.out.println("");
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
			System.out.println("");
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
			System.out.println(res);
			print_result obj = new print_result(graph, res, name_key);
			obj.printTable();
		}

		graph = null;
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
