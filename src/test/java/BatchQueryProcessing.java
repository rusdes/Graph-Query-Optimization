import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import org.javatuples.Pair;
import org.javatuples.Quintet;
import org.javatuples.Triplet;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import me.tongfei.progressbar.ProgressBar;
import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;
import operators.helper.print_result;
import queryplan.CostBasedOptimzer;
import queryplan.StatisticsCollector;
import queryplan.StatisticsTransformation;
import queryplan.querygraph.QueryEdge;
import queryplan.querygraph.QueryGraph;
import queryplan.querygraph.QueryVertex;

class Query {
    Long queryId;
    String queryTag;
    QueryGraph queryGraph;

    public Long getQueryId() {
        return queryId;
    }

    public void setQueryId(Long queryId) {
        this.queryId = queryId;
    }

    public String getQueryTag() {
        return queryTag;
    }

    public void setQueryTag(String queryTag) {
        this.queryTag = queryTag;
    }

    public QueryGraph getQueryGraph() {
        return queryGraph;
    }

    public void setQueryGraph(QueryGraph queryGraph) {
        this.queryGraph = queryGraph;
    }
}

public class BatchQueryProcessing {

    public static HashMap<String, List<Query>> LoadQueries(Path path)
            throws Exception {
        System.out.println("\nLoading Queries...");
        HashMap<String, List<Query>> queries = new HashMap<>();
        List<Query> simple = new ArrayList<>();
        List<Query> medium = new ArrayList<>();
        List<Query> complex = new ArrayList<>();
        try (Reader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path.toString()), "utf-8"))) {
            try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1)
                    .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
                    .build()) {
                String[] line;
                List<QueryEdge> qe = new ArrayList<>();
                List<QueryVertex> qv = new ArrayList<>();
                int mediumCount = 0;
                int complexCount = 0;
                while ((line = csvReader.readNext()) != null) {
                    long Id = Long.parseLong(line[0]);

                    HashMap<String, Pair<String, String>> personProps = new HashMap<>();
                    String[] pprops = line[2].split(",");
                    if (pprops.length > 1) {
                        for (int i = 0; i < pprops.length - 1; i = i + 3) {
                            personProps.put(pprops[i], new Pair<String, String>(pprops[i + 1], pprops[i + 2]));
                        }
                    } else {
                        personProps = new HashMap<String, Pair<String, String>>();
                    }
                    QueryVertex person = new QueryVertex(line[1], personProps, true);

                    HashMap<String, Pair<String, String>> movieProps = new HashMap<>();
                    String[] mprops = line[4].split(",");
                    if (mprops.length > 1) {
                        for (int i = 0; i < mprops.length - 1; i = i + 3) {
                            movieProps.put(mprops[i], new Pair<String, String>(mprops[i + 1], mprops[i + 2]));
                        }
                    } else {
                        movieProps = new HashMap<String, Pair<String, String>>();
                    }

                    QueryVertex movie = new QueryVertex(line[3], movieProps, true);

                    String edgeLabel = line[5];
                    String tag = line[6];

                    QueryEdge pm = new QueryEdge(person, movie, edgeLabel, new HashMap<String, Pair<String, String>>());

                    if (tag.equals("simple")) {
                        qv.add(movie);
                        qv.add(person);
                        qe.add(pm);
                        Query q = new Query();
                        q.setQueryId(Id);
                        q.setQueryTag(tag);
                        QueryVertex[] x = new QueryVertex[qv.size()];
                        QueryEdge[] y = new QueryEdge[qe.size()];
                        for (int i = 0; i < qv.size(); i++) {
                            x[i] = qv.get(i);
                        }
                        for (int i = 0; i < qe.size(); i++) {
                            y[i] = qe.get(i);
                        }
                        q.setQueryGraph(new QueryGraph(x, y));
                        qv.clear();
                        qe.clear();
                        simple.add(q);

                    } else if (tag.equals("medium")) {
                        qv.add(movie);
                        qv.add(person);
                        qe.add(pm);
                        mediumCount++;

                        if (mediumCount % 3 == 0) {
                            Query q = new Query();
                            q.setQueryId(Id);
                            q.setQueryTag(tag);
                            QueryVertex[] x = new QueryVertex[qv.size()];
                            QueryEdge[] y = new QueryEdge[qe.size()];
                            for (int i = 0; i < qv.size(); i++) {
                                x[i] = qv.get(i);
                            }
                            for (int i = 0; i < qe.size(); i++) {
                                y[i] = qe.get(i);
                            }
                            q.setQueryGraph(new QueryGraph(x, y));
                            qv.clear();
                            qe.clear();
                            medium.add(q);
                        }

                    } else if (tag.equals("complex")) {
                        qv.add(movie);
                        qv.add(person);
                        qe.add(pm);
                        complexCount++;

                        if (complexCount % 6 == 0) {
                            Query q = new Query();
                            q.setQueryId(Id);
                            q.setQueryTag(tag);
                            QueryVertex[] x = new QueryVertex[qv.size()];
                            QueryEdge[] y = new QueryEdge[qe.size()];
                            for (int i = 0; i < qv.size(); i++) {
                                x[i] = qv.get(i);
                            }
                            for (int i = 0; i < qe.size(); i++) {
                                y[i] = qe.get(i);
                            }
                            q.setQueryGraph(new QueryGraph(x, y));
                            qv.clear();
                            qe.clear();
                            complex.add(q);
                        }

                    }
                }
            }
        }
        queries.put("simple", simple);
        queries.put("medium", medium);
        queries.put("complex", complex);
        return queries;
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

    public static EdgeExtended EdgeFromFileToDataSet(
            Quintet<Long, Long, Long, String, String> edgeFromFile) {

        EdgeExtended edge = new EdgeExtended();

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

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        String queryDir = null;
        String dir = null;
        // String name_key = "name";
        int choice = 23;
        switch (choice) {
            case 11: {
                dir = "src/test/java/Dataset/IMDB/IMDB_Small";
                queryDir = "src/test/java/Queries/IMDB";
                break;
            }

            case 12: {
                dir = "src/test/java/Dataset/IMDB/IMDB_Medium";
                queryDir = "src/test/java/Queries/IMDB";
                break;
            }
            
            case 13: {
                dir = "src/test/java/Dataset/IMDB/IMDB_Large";
                queryDir = "src/test/java/Queries/IMDB";
                break;
            }

            case 21: {
				dir = "src/test/java/Dataset/DBLP/DBLP_Small";
                queryDir = "src/test/java/Queries/DBLP";
				break;
			}

			case 22: {
				dir = "src/test/java/Dataset/DBLP/DBLP_Medium";
                queryDir = "src/test/java/Queries/DBLP";
				break;
			}

            case 23: {
				dir = "src/test/java/Dataset/DBLP/DBLP_Large";
                queryDir = "src/test/java/Queries/DBLP";
				break;
			}
        }
        

        int totalQueries = 0;
        HashMap<String, List<Query>> queries = LoadQueries(Paths.get(queryDir, "queries.csv"));
        System.out.println("Simple, Medium and Complex QueryGraph Buckets generated\n");
        System.out.println("Difficulty\tCount\n-----------------------");
        for (String difficulty : queries.keySet()) {
            System.out.println(difficulty + "\t\t" + queries.get(difficulty).size());
            totalQueries += queries.get(difficulty).size();
        }
        System.out.println();

        
        Boolean compare = true;
        Set<String> options = new HashSet<>();
        options.addAll(Arrays.asList("vertex_naive", "edges_naive"));

        // Description for all options
        HashMap<String, ArrayList<String>> desc = new HashMap<>();
        desc.put("Initial Vertex Mapping Method", new ArrayList<>(Arrays.asList("vertex_naive", "vertex_kdtree")));
        desc.put("Edges Mapping Method", new ArrayList<>(Arrays.asList("edges_kdtree", "edges_kdtree")));
        desc.put("KDTree Type", new ArrayList<>(Arrays.asList("unbalanced_kdtree", "balanced_kdtree")));
        desc.put("Edge Properties Present", new ArrayList<>(Arrays.asList("no_edge_properties", "edge_properties")));


        String srcDir = dir;
        String tarDir = dir + "/Dataset_Statistics";
        File theDir = new File(tarDir);
        if (!theDir.exists()) {
            theDir.mkdirs();
            // Write statistics to file if file is not present in tarDir
            StatisticsCollector stats = new StatisticsCollector(srcDir, tarDir);
            stats.collect();
            stats = null; // Free up memory
        }

        List<Triplet<Long, String, String>> verticesFromFile = readVerticesLineByLine(Paths.get(dir, "vertices.csv"));
        List<Quintet<Long, Long, Long, String, String>> edgesFromFile = readEdgesLineByLine(
                Paths.get(dir, "edges.csv"));

        List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> vertices = verticesFromFile.stream()
                .map(elt -> VertexFromFileToDataSet(elt)).collect(Collectors.toList());

        List<EdgeExtended> edges = edgesFromFile.stream()
                .map(elt -> EdgeFromFileToDataSet(elt))
                .collect(Collectors.toList());

        StatisticsTransformation sts = new StatisticsTransformation(tarDir);
        HashMap<String, Pair<Long, Double>> vstat = sts.getVerticesStatistics();
        HashMap<String, Pair<Long, Double>> estat = sts.getEdgesStatistics();
        HashMap<String, HashMap<String, Long>> finalResultTable = new HashMap<>();

        GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph_unbal;
        GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph_bal;

        graph_unbal = GraphExtended.fromList(vertices, edges, new HashSet<>(Arrays.asList("unbalanced_kdtree")),
                dir);
        graph_bal = GraphExtended.fromList(vertices, edges, new HashSet<>(Arrays.asList("balanced_kdtree")),
                dir);

        // Internal HashMap Keys
        // VnEn - Vertex Naive, Edge Naive
        // VnEkUbalEk - Vertex Naive, Edge KDTree, Unbalanced Edge KDTree
        // VnEkBalEk - Vertex Naive, Edge KDTree, Balanced Edge KDTree
        // VkEnUbalVk - Vertex KDtree, Edge Naive, Unbalanced Vertex KDTree
        // VkEnBalVk - Vertex KDtree, Edge Naive, Balanced Vertex KDTree
        // VkEkUbalVEk - Vertex KDtree, Edge KDtree, Unbalanced Vertex & Edge KDTree
        // VkEkBalVEk - Vertex KDtree, Edge KDtree, Balanced Vertex & Edge KDTree

        ForkJoinPool myPool = new ForkJoinPool(4);

        myPool.submit(() -> {
            queries.keySet().parallelStream().forEach((difficulty) -> {

                HashMap<String, Long> internalMap = new HashMap<>();
                Long time_VnEn = 0L;
                Long time_VnEkUbalEk = 0L;
                Long time_VnEkBalEk = 0L;
                Long time_VkEnUbalVk = 0L;
                Long time_VkEnBalVk = 0L;
                Long time_VkEkUbalVEk = 0L;
                Long time_VkEkBalVEk = 0L;
                int qcount = 0;

                // Serial
                int numQueries = queries.get(difficulty).size();

                try (ProgressBar pb = new ProgressBar(
                        ("Query (" + difficulty + ") on " + Thread.currentThread().getName().substring(15)),
                        numQueries)) {

                    for (Query query : queries.get(difficulty)) {
                        pb.step();

                        QueryGraph g = query.getQueryGraph();

                        CostBasedOptimzer pg_unbal;
                        CostBasedOptimzer pg_bal;

                        List<List<Long>> res = new ArrayList<>();

                        pg_unbal = new CostBasedOptimzer(g, graph_unbal, vstat, estat);

                        pg_bal = new CostBasedOptimzer(g, graph_bal, vstat, estat);

                        if (compare) {
                            long startTime, endTime;

                            // Vertex Naive, Edge Naive
                            Set<String> options1 = new HashSet<>(Arrays.asList("vertex_naive", "edges_naive"));

                            startTime = System.nanoTime();
                            for (int i = 0; i < 1; i++) {
                                try {
                                    res = pg_unbal.generateQueryPlan(options1);
                                } catch (Exception e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                            }
                            endTime = System.nanoTime();
                            time_VnEn += (endTime - startTime) / 1000000;

                            // Vertex Naive, Edge KDTree, Unbalanced Edge KDTree
                            options1 = new HashSet<>(Arrays.asList("vertex_naive", "edges_kdtree"));
                            startTime = System.nanoTime();
                            for (int i = 0; i < 1; i++) {
                                try {
                                    res = pg_unbal.generateQueryPlan(options1);
                                } catch (Exception e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                            }
                            endTime = System.nanoTime();
                            time_VnEkUbalEk += (endTime - startTime) / 1000000;

                            // Vertex Naive, Edge KDTree, Balanced Edge KDTree
                            options1 = new HashSet<>(Arrays.asList("vertex_naive", "edges_kdtree", "balanced_kdtree"));
                            startTime = System.nanoTime();
                            for (int i = 0; i < 1; i++) {
                                try {
                                    res = pg_bal.generateQueryPlan(options1);
                                } catch (Exception e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                            }
                            endTime = System.nanoTime();
                            time_VnEkBalEk += (endTime - startTime) / 1000000;

                            // Vertex KDtree, Edge Naive, Unbalanced Vertex KDTree
                            options1 = new HashSet<>(
                                    Arrays.asList("vertex_kdtree", "edges_naive", "unbalanced_kdtree"));
                            startTime = System.nanoTime();
                            for (int i = 0; i < 1; i++) {
                                try {
                                    res = pg_unbal.generateQueryPlan(options1);
                                } catch (Exception e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                            }
                            endTime = System.nanoTime();
                            time_VkEnUbalVk += (endTime - startTime) / 1000000;

                            // Vertex KDtree, Edge Naive, Balanced Vertex KDTree
                            options1 = new HashSet<>(Arrays.asList("vertex_kdtree", "edges_naive", "balanced_kdtree"));
                            startTime = System.nanoTime();
                            for (int i = 0; i < 1; i++) {
                                try {
                                    res = pg_bal.generateQueryPlan(options1);
                                } catch (Exception e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                            }
                            endTime = System.nanoTime();
                            time_VkEnBalVk += (endTime - startTime) / 1000000;

                            // Vertex KDtree, Edge KDtree, Unbalanced Vertex & Edge KDTree
                            options1 = new HashSet<>(
                                    Arrays.asList("vertex_kdtree", "edges_kdtree", "unbalanced_kdtree"));
                            startTime = System.nanoTime();
                            for (int i = 0; i < 1; i++) {
                                try {
                                    res = pg_unbal.generateQueryPlan(options1);
                                } catch (Exception e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                            }
                            endTime = System.nanoTime();
                            time_VkEkUbalVEk += (endTime - startTime) / 1000000;

                            // Vertex KDtree, Edge KDtree, Balanced Vertex & Edge KDTree
                            options1 = new HashSet<>(Arrays.asList("vertex_kdtree", "edges_kdtree", "balanced_kdtree"));
                            startTime = System.nanoTime();
                            for (int i = 0; i < 1; i++) {
                                try {
                                    res = pg_bal.generateQueryPlan(options1);
                                } catch (Exception e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                            }
                            endTime = System.nanoTime();
                            time_VkEkBalVEk += (endTime - startTime) / 1000000;
                        } else {
                            System.out.println(options);
                            if (options.contains("balanced_kdtree")) {
                                try {
                                    res = pg_bal.generateQueryPlan(options);
                                } catch (Exception e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                            } else {
                                try {
                                    res = pg_unbal.generateQueryPlan(options);
                                } catch (Exception e) {
                                    // TODO Auto-generated catch block
                                    e.printStackTrace();
                                }
                            }
                        }
                        qcount++;
                        // System.out.println("Done");
                        if (qcount == numQueries)
                            break;
                    }
                }

                internalMap.put("VNEN", time_VnEn / qcount);
                internalMap.put("VNEKUbalEK", time_VnEkUbalEk / qcount);
                internalMap.put("VNEKBalEK", time_VnEkBalEk / qcount);
                internalMap.put("VKENUbalVK", time_VkEnUbalVk / qcount);
                internalMap.put("VKENBalVK", time_VkEnBalVk / qcount);
                internalMap.put("VKEKUbalVEK", time_VkEkUbalVEk / qcount);
                internalMap.put("VKEKBalVEK", time_VkEkBalVEk / qcount);
                finalResultTable.put(difficulty, internalMap);
            });
        }).get();

        // }
        System.out.println();
        System.out.println("\nQuery Execution Completed: Executed " + totalQueries + " Queries");
        writeResultsToCSV(finalResultTable, queryDir);

    }

    public static void writeResultsToCSV(HashMap<String, HashMap<String, Long>> data, String filePath) {
        // first create file object for file placed at location
        // specified by filepath
        try {

            // create FileWriter object with file as parameter
            System.out.print("\nGenerating results.csv: ");
            filePath = filePath + "/results.csv";
            PrintWriter writer = new PrintWriter(filePath);
            List<String> executionStrategy = new ArrayList<>();
            executionStrategy.add("VNEN");
            executionStrategy.add("VNEKUbalEK");
            executionStrategy.add("VNEKBalEK");
            executionStrategy.add("VKENUbalVK");
            executionStrategy.add("VKENBalVK");
            executionStrategy.add("VKEKUbalVEK");
            executionStrategy.add("VKEKBalVEK");

            writer.println("Difficulty|VNEN|VNEKUbalEK|VNEKBalEK|VKENUbalVK|VKENBalVK|VKEKUbalVEK|VKEKBalVEK");

            for (String difficulty : data.keySet()) {
                String toBePrinted = difficulty;
                for (String strategy : executionStrategy) {
                    toBePrinted += "|" + data.get(difficulty).get(strategy);
                }
                writer.println(toBePrinted);
            }

            // closing writer connection
            writer.close();
            System.out.println("Done");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
