package operators.datastructures;

import operators.datastructures.EdgeExtended;
import operators.datastructures.GraphExtended;
import operators.datastructures.VertexExtended;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class KDNode {
    VertexExtended<Long, HashSet<String>, HashMap<String, String>> current;
    VertexExtended<Long, HashSet<String>, HashMap<String, String>> left;
    VertexExtended<Long, HashSet<String>, HashMap<String, String>> right;
    int nodeLevel;

    public int getNodeLevel() {
        return nodeLevel;
    }

    public void setNodeLevel(int nodeLevel) {
        this.nodeLevel = nodeLevel;
    }

    public VertexExtended<Long, HashSet<String>, HashMap<String, String>> getCurrent() {
        return current;
    }

    public void setCurrent(VertexExtended<Long, HashSet<String>, HashMap<String, String>> current) {
        this.current = current;
    }

    public VertexExtended<Long, HashSet<String>, HashMap<String, String>> getLeft() {
        return left;
    }

    public void setLeft(VertexExtended<Long, HashSet<String>, HashMap<String, String>> left) {
        this.left = left;
    }

    public VertexExtended<Long, HashSet<String>, HashMap<String, String>> getRight() {
        return right;
    }

    public void setRight(VertexExtended<Long, HashSet<String>, HashMap<String, String>> right) {
        this.right = right;
    }
}
