package queryplan;

import queryplan.querygraph.QueryEdge;
import queryplan.querygraph.QueryGraph;
import queryplan.querygraph.QueryVertex;
import operators.datastructures.KDTree;
import operators.datastructures.KDNode;

import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class CustomOptimizer {
    GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph;
    List<KDTree> trees = new ArrayList<>();

    public CustomOptimizer(
            GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph) {
        this.graph = graph;
    }

    public void preprocess() {
        List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> vertices = graph.getVertices();
        HashMap<HashSet<String>, List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>>> classes = new HashMap<>();

        for (VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertexExtended : vertices) {
            try {
                classes.get(vertexExtended.getLabels()).add(vertexExtended);

            } catch (Exception e) {
                List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> v = new ArrayList<>();
                v.add(vertexExtended);
                classes.put(vertexExtended.getLabels(), v);
            }
        }
        for (HashSet<String> label : classes.keySet()) {
            List<String> dimensions = new ArrayList<>(classes.get(label).get(0).getProps().keySet());
            HashSet<String> treeLabel = label;
            KDTree x = new KDTree(dimensions, treeLabel);
            for (VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex : classes.get(label)) {
                x.addNode(vertex);
            }

            trees.add(x);
        }
        System.out.println(trees);
    }

}
