package API;

import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;

import org.javatuples.*;
import org.json.simple.JSONObject;

// import com.fasterxml.jackson.databind.util.JSONPObject;
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
import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

/*
{"dataset": "Toy", "nodes": [{"id": 1, "label": "Artist", "props": [{"key": "Name", "value": "Canela Cox", "op": "eq"}], "retValue": "True"}, {"id": 2, "label": "Band", "props": [{}], "retValue": "True"}], "edges": [{"from": 1, "to": 2, "label": "Part Of", "props": [{}]}]}

 curl -X POST -H "Content-type: application/json" -d '{"dataset": "Toy", "nodes": [{"id": 1, "label": "Artist", "props": [{"key": "Name", "value": "Canela Cox", "op": "eq"}], "retValue": "True"}, {"id": 2, "label": "Band", "props": [{}], "retValue": "True"}], "edges": [{"from": 1, "to": 2, "label": "Part Of", "props": [{}]}]}' "http://localhost:8080/query"
 */

class QueryData {
    private String dataset;
    private List<JSONObject> nodes;
    private List<JSONObject> edges;

    public String getDataset() {
        return dataset;
    }

    public List<JSONObject> getNodes() {
        return nodes;
    }

    public List<JSONObject> getEdges() {
        return edges;
    }

    public QueryGraph getQueryGraph() {
        QueryVertex[] vs = new QueryVertex[nodes.size()];
        QueryEdge[] es = new QueryEdge[edges.size()];

        int counter = 0;
        for (JSONObject node : nodes) {
            HashMap<String, Pair<String, String>> props = new HashMap<String, Pair<String, String>>();

            ArrayList<Object> ja = (ArrayList) node.get("props");
            int id = (int) node.get("id");
            String label = (String) node.get("label");
            String rv = (String) node.get("retValue");

            for (Object hashMap : ja) {
                LinkedHashMap lMap = (LinkedHashMap) hashMap;
                if (lMap.isEmpty()) {
                    break;
                }

                String key = (String) lMap.get("key");
                Pair<String, String> p = new Pair<String, String>((String) lMap.get("op"), (String) lMap.get("value"));
                props.put(key, p);
            }

            vs[id] = new QueryVertex(label, props, rv.equals("True") ? true : false);
            counter++;
        }

        counter = 0;
        for (JSONObject edge : edges) {
            HashMap<String, Pair<String, String>> props = new HashMap<String, Pair<String, String>>();

            ArrayList<Object> ja = (ArrayList) edge.get("props");
            String label = (String) edge.get("label");
            int from = (int) edge.get("from");
            int to = (int) edge.get("to");

            for (Object hashMap : ja) {
                LinkedHashMap lMap = (LinkedHashMap) hashMap;
                if (lMap.isEmpty()) {
                    break;
                }

                String key = (String) lMap.get("key");
                Pair<String, String> p = new Pair<String, String>((String) lMap.get("op"), (String) lMap.get("value"));
                props.put(key, p);
            }

            es[counter] = new QueryEdge(vs[from], vs[to], label, props);
            counter++;
        }
        return new QueryGraph(vs, es);
    }
}

class Result {
    List<Double> runtime;
    List<List<List<String>>> resultsRet;

    public Result(List<Double> l, List<List<List<String>>> r) {
        this.runtime = l;
        this.resultsRet = r;
    }
}

@CrossOrigin(origins = { "*" }, maxAge = 4800, allowCredentials = "false")
@RestController
public class API {
    @PostMapping("/query")
    @ResponseBody
    public Map<String, Object> query(@RequestBody QueryData queryData) throws Exception {

        // Boolean labeled = true; // change this to false to run on unlabeled data
        // System.out.println(
        // "1 - Compressed IMDB\n2 - Uncompressed IMDB\n3 - Unlabeled Toy Dataset\n4 -
        // Labeled Toy Dataset");

        String choice = queryData.getDataset();

        String dir = null;
        Set<String> options = new HashSet<>();
        String name_key = null;

        // Description for all options
        HashMap<String, ArrayList<String>> desc = new HashMap<>();
        desc.put("Initial Vertex Mapping Method", new ArrayList<>(Arrays.asList("vertex_naive", "vertex_kdtree")));
        desc.put("Edges Mapping Method", new ArrayList<>(Arrays.asList("edges_kdtree", "edges_kdtree")));
        desc.put("KDTree Type", new ArrayList<>(Arrays.asList("unbalanced_kdtree", "balanced_kdtree")));

        switch (choice) {
            case "IMDB": {
                // Query- 21, 22, 24
                // dir = "src/test/java/Dataset/compressed_imdb";
                dir = "src/test/java/Dataset/IMDB_Small";
                name_key = "name";
                break;
            }

            case "Toy": {
                // toy dataset
                // Query: 40, 41, 42, 43, 44, 45
                dir = "src/test/java/Dataset/Toy";
                name_key = "Name";
                break;
            }
        }

        // defining source and target path for statistics files of edge and vertices
        String tarDir = dir + "/Dataset_Statistics";

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

        GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph_unbal;
        GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph_bal;

        CostBasedOptimzer pg_unbal;
        CostBasedOptimzer pg_bal;

        List<List<Long>> res = new ArrayList<>();

        QueryGraph g = queryData.getQueryGraph();
        System.out.println("Query:");
        System.out.println(g.getQueryVertices());
        System.out.println(g.getQueryEdges());

        // Garbage Collector
        verticesFromFile = null;
        edgesFromFile = null;

        int numTimes = 1000;

        graph_unbal = GraphExtended.fromList(vertices, edges, new HashSet<>(Arrays.asList("unbalanced_kdtree")), dir);
        pg_unbal = new CostBasedOptimzer(g, graph_unbal, vstat, estat);

        graph_bal = GraphExtended.fromList(vertices, edges, new HashSet<>(Arrays.asList("balanced_kdtree")), dir);
        pg_bal = new CostBasedOptimzer(g, graph_bal, vstat, estat);

        System.out.println("Initialization Finished. \nStarting Query Execution...");
        List<Float> runtime = new ArrayList<>();
        long startTime, endTime;

        // Vertex Naive, Edge Naive
        System.out.println("\nVertex Naive, Edge Naive: ");

        options = new HashSet<>(Arrays.asList("vertex_naive", "edges_naive"));

        startTime = System.nanoTime();
        for (int i = 0; i < numTimes; i++) {
            res = pg_unbal.generateQueryPlan(options);
        }
        endTime = System.nanoTime();
        System.out.println(res);

        runtime.add((float) ((endTime - startTime) / (float) numTimes) / 1000000);
        System.out.println("time(ms): " + (float) ((endTime - startTime) / (float) numTimes) / 1000000);

        // Vertex Naive, Edge KDTree, Unbalanced Edge KDTree
        System.out.println("\nVertex Naive, Edge KDtree, Unbalanced Edge KDTree: ");

        options = new HashSet<>(Arrays.asList("vertex_naive", "edges_kdtree"));

        startTime = System.nanoTime();
        for (int i = 0; i < numTimes; i++) {
            res = pg_unbal.generateQueryPlan(options);
        }
        endTime = System.nanoTime();

        System.out.println(res);

        runtime.add((float) ((endTime - startTime) / (float) numTimes) / 1000000);
        System.out.println("time(ms): " + (float) ((endTime - startTime) / (float) numTimes) / 1000000);

        // Vertex Naive, Edge KDTree, Balanced Edge KDTree
        System.out.println("\nVertex Naive, Edge KDtree, Balanced Edge KDTree: ");

        options = new HashSet<>(Arrays.asList("vertex_naive", "edges_kdtree", "balanced_kdtree"));

        startTime = System.nanoTime();
        for (int i = 0; i < numTimes; i++) {
            res = pg_bal.generateQueryPlan(options);
        }

        endTime = System.nanoTime();
        System.out.println(res);

        runtime.add((float) ((endTime - startTime) / (float) numTimes) / 1000000);
        System.out.println("time(ms): " + (float) ((endTime - startTime) / (float) numTimes) / 1000000);

        // Vertex KDtree, Edge Naive, Unbalanced Vertex KDTree
        System.out.println("\nVertex KDtree, Edge Naive, Unbalanced Vertex KDTree: ");

        options = new HashSet<>(Arrays.asList("vertex_kdtree", "edges_naive", "unbalanced_kdtree"));

        startTime = System.nanoTime();
        for (int i = 0; i < numTimes; i++) {
            res = pg_unbal.generateQueryPlan(options);
        }

        endTime = System.nanoTime();
        System.out.println(res);

        runtime.add((float) ((endTime - startTime) / (float) numTimes) / 1000000);
        System.out.println("time(ms): " + (float) ((endTime - startTime) / (float) numTimes) / 1000000);

        // Vertex KDtree, Edge Naive, Balanced Vertex KDTree
        System.out.println("\nVertex KDtree, Edge Naive, Balanced Vertex KDTree: ");

        options = new HashSet<>(Arrays.asList("vertex_kdtree", "edges_naive", "balanced_kdtree"));

        startTime = System.nanoTime();
        for (int i = 0; i < numTimes; i++) {
            res = pg_bal.generateQueryPlan(options);
        }

        endTime = System.nanoTime();
        System.out.println(res);

        runtime.add((float) ((endTime - startTime) / (float) numTimes) / 1000000);
        System.out.println("time(ms): " + (float) ((endTime - startTime) / (float) numTimes) / 1000000);

        // Vertex KDtree, Edge KDtree, Unbalanced Vertex & Edge KDTree
        System.out.println("\nVertex KDtree, Edge KDtree, Unbalanced Both KDTrees");

        options = new HashSet<>(Arrays.asList("vertex_kdtree", "edges_kdtree", "unbalanced_kdtree"));

        startTime = System.nanoTime();
        for (int i = 0; i < numTimes; i++) {
            res = pg_unbal.generateQueryPlan(options);
        }
        endTime = System.nanoTime();
        System.out.println(res);

        runtime.add((float) ((endTime - startTime) / (float) numTimes) / 1000000);
        System.out.println("time(ms): " + (float) ((endTime - startTime) / (float) numTimes) / 1000000);

        // Vertex KDtree, Edge KDtree, Balanced Vertex & Edge KDTree
        System.out.println("\nVertex KDtree, Edge KDtree, Balanced Both KDTrees");

        options = new HashSet<>(Arrays.asList("vertex_kdtree", "edges_kdtree", "balanced_kdtree"));

        startTime = System.nanoTime();
        for (int i = 0; i < numTimes; i++) {
            res = pg_bal.generateQueryPlan(options);
        }
        endTime = System.nanoTime();
        System.out.println(res);

        runtime.add((float) ((endTime - startTime) / (float) numTimes) / 1000000);
        System.out.println("time(ms): " + (float) ((endTime - startTime) / (float) numTimes) / 1000000);

        List<List<List<String>>> resultsRet = new ArrayList<>();
        for (int row = 0; row < res.size(); row++) {
            List<List<String>> intermed = new ArrayList<>();
            for (int col = 0; col < res.get(0).size(); col++) {
                VertexExtended<Long, HashSet<String>, HashMap<String, String>> v = graph_bal
                        .getVertexByID(res.get(row).get(col));
                List<String> l = new ArrayList<>();
                l.add(v.getLabel());
                l.add(v.getProps().get(name_key));
                intermed.add(l);
            }
            resultsRet.add(intermed);
        }

        System.out.println();
        System.out.println(res);

        Map<String, Object> object = new HashMap<>();
        object.put("runtime", runtime);
        object.put("results", resultsRet);
        // return new ResponseEntity<List<List<List<String>>>>(resultsRet,
        // HttpStatus.OK);
        return object;

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
}
