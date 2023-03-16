package operators.helper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;

public class GraphCompressor {

    public void compress(
            GraphExtended<Long, HashSet<String>, HashMap<String, String>, Long, String, HashMap<String, String>> graph) {
        List<EdgeExtended<Long, Long, String, HashMap<String, String>>> edgesToBeCompressed = new ArrayList<>();
        Collection<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> vertices = graph.getVertices();
        Collection<EdgeExtended<Long, Long, String, HashMap<String, String>>> edges = graph.getEdges();
        boolean isCompressible = true;
        System.out.println("Checking Dataset for Compressibility");
        for (VertexExtended<Long, HashSet<String>, HashMap<String, String>> vertex : vertices) {
            if (!vertex.getProps().isEmpty()) {
                isCompressible = false;
                break;
            }
        }

        if (isCompressible) {
            System.out.println("Initiating Compression");
            Set<Long> sourceIds = new HashSet<>();
            for (EdgeExtended<Long, Long, String, HashMap<String, String>> edge : edges) {
                sourceIds.add(edge.getSourceId());
            }

            for (EdgeExtended<Long, Long, String, HashMap<String, String>> edge : edges) {
                if (!sourceIds.contains(edge.getTargetId())) {
                    edgesToBeCompressed.add(edge);
                }
            }

            for (int i = 0; i < edgesToBeCompressed.size(); i++) {
                if (edgesToBeCompressed.get(i) != null) {
                    boolean incompatible = false;
                    for (int j = i + 1; j < edgesToBeCompressed.size(); j++) {
                        if (edgesToBeCompressed.get(j) != null) {
                            if (edgesToBeCompressed.get(i).getSourceId().equals(edgesToBeCompressed.get(j).getSourceId())
                                    && edgesToBeCompressed.get(i).getLabel()
                                            .equals(edgesToBeCompressed.get(j).getLabel())) {
                                edgesToBeCompressed.set(j, null);
                                incompatible = true;
                            }
                        }
                    }
                    if (incompatible) {
                        edgesToBeCompressed.set(i, null);
                    }
                }
            }

            Set<Long> verticesToBeDeleted = new HashSet<>();
            Set<Long> edgesToBeDeleted = new HashSet<>();
            for (EdgeExtended<Long, Long, String, HashMap<String, String>> toBeCompressed : edgesToBeCompressed) {
                if (toBeCompressed != null) {
                    VertexExtended<Long, HashSet<String>, HashMap<String, String>> source = graph
                            .getVertexByID(toBeCompressed.getSourceId());
                    HashMap<String, String> newProps = source.getProps();
                    newProps.put(toBeCompressed.getLabel(),
                            graph.getVertexByID(toBeCompressed.getTargetId()).getLabel());
                    source.setProps(newProps);
                    edgesToBeDeleted.add(toBeCompressed.getEdgeId());
                    verticesToBeDeleted.add(toBeCompressed.getTargetId());
                }
            }

            int nodePruneCount = verticesToBeDeleted.size();
            int edgePruneCount = edgesToBeDeleted.size();

            // for (Long e : edgesToBeDeleted) {
            // graph.deleteEdgeById(e);
            // }

            // for (Long v : verticesToBeDeleted) {
            // graph.deleteVertexById(v);
            // }

            System.out.println(
                    "Compression Completed - Pruned " + nodePruneCount + " nodes and " + edgePruneCount + " edges\n");
        } else {
            System.out.println("Compression is not required. Skipping Compression\n");
        }
    }
}
