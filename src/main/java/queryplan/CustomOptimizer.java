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
        System.out.println(classes.size());
        for (HashSet<String> label : classes.keySet()) {
            VertexExtended<Long, HashSet<String>, HashMap<String, String>> firstVertexInBucket = classes.get(label)
                    .get(0);
            List<String> dimensions = new ArrayList<>(classes.get(label).get(0).getProps().keySet());
            HashSet<String> treeLabel = label;
            KDNode root = new KDNode();
            root.setNodeLevel(0);
            root.setCurrent(firstVertexInBucket);
            KDTree x = new KDTree(dimensions, treeLabel, root);
            trees.add(x);
        }
        System.out.println(trees);
    }

}
