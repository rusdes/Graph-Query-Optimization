package operators.datastructures;

import java.util.Arrays;
import java.util.Comparator;


import org.javatuples.Pair;

import operators.datastructures.kdtree.KDTree;
import static operators.datastructures.kdtree.Constants.STRING_MIN_VALUE;
import static operators.datastructures.kdtree.Constants.STRING_MAX_VALUE;

class Sort1 implements Comparator<Pair<String[], String>> {
    int dim;

    public Sort1(int dim) {
        this.dim = dim;
    }

    public int compare(Pair<String[], String> a, Pair<String[], String> b) {
        return (a.getValue0())[this.dim].compareTo(b.getValue0()[this.dim]);
    }
}

public class kd_exp {
    public static void main(String[] args) {
        KDTree kdTree = new KDTree(2); // 2 dimensions (x, y)
        // point insertion:
        kdTree.insert(new String[] { "James", "45" }, "Artist1");
        kdTree.insert(new String[] { "Roy", "70" }, "Artist2");
        kdTree.insert(new String[] { "Rushil", "50" }, "Artist3");
        kdTree.insert(new String[] { "Alan", "10" }, "Artist4");
        kdTree.insert(new String[] { "Jake", "90" }, "Artist5");
        kdTree.insert(new String[] { "Miley", "85" }, "Artist6");

        Object[] output = kdTree.range(new String[] { "Roy", STRING_MIN_VALUE },
                new String[] { STRING_MAX_VALUE, Integer.toString(80) });
        System.out.println("Output = " + Arrays.toString(output));
        System.out.println("");
        System.out.println(kdTree.toString()); // check the data

        Pair[] arr = { new Pair<String[], String>(new String[] { "James", "45" }, "Artist1"),
                new Pair<String[], String>(new String[] { "Roy", "70" }, "Artist2"),
                new Pair<String[], String>(new String[] { "Rushil", "50" }, "Artist3"),
                new Pair<String[], String>(new String[] { "Alan", "10" }, "Artist4"),
                new Pair<String[], String>(new String[] { "Jake", "90" }, "Artist5"),
                new Pair<String[], String>(new String[] { "Miley", "85" }, "Artist6"),
        };

        Arrays.sort(arr, new Sort1(0));

    }
}
