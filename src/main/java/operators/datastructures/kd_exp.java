package operators.datastructures;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.javatuples.Pair;

import operators.datastructures.kdtree.KDTree;
import static operators.datastructures.kdtree.Constants.STRING_MIN_VALUE;
import static operators.datastructures.kdtree.Constants.STRING_MAX_VALUE;

class Tuple<R, S> implements Comparator<Tuple<String[], String>> {
    public R key;
    public S label;
    int dim = 0;

    public Tuple(R r, S s) {
        this.key = r;
        this.label = s;
        // this.dim = dim;
    }

    @Override
    public int compare(Tuple<String[], String> a, Tuple<String[], String> b) {
        return (a.key)[this.dim].compareTo(b.key[this.dim]);
    }
}

class Sort1 implements Comparator<Tuple<String[], String>> {
    int dim;

    public Sort1(int dim) {
        this.dim = dim;
    }

    @Override
    public int compare(Tuple<String[], String> a, Tuple<String[], String> b) {
        return (a.key)[this.dim].compareTo(b.key[this.dim]);
    }
}

public class kd_exp {
    public static void main(String[] args) {
        // KDTree kdTree = new KDTree(2); // 2 dimensions (x, y)
        // // point insertion:
        // kdTree.insert(new String[] { "James", "45" }, "Artist1");
        // kdTree.insert(new String[] { "Roy", "70" }, "Artist2");
        // kdTree.insert(new String[] { "Rushil", "50" }, "Artist3");
        // kdTree.insert(new String[] { "Alan", "10" }, "Artist4");
        // kdTree.insert(new String[] { "Jake", "90" }, "Artist5");
        // kdTree.insert(new String[] { "Miley", "85" }, "Artist6");

        // Object[] output = kdTree.range(new String[] { "Roy", STRING_MIN_VALUE },
        //         new String[] { STRING_MAX_VALUE, Integer.toString(80) });
        // System.out.println("Output = " + Arrays.toString(output));
        // System.out.println("");
        // System.out.println(kdTree.toString()); // check the data

        List<Tuple> arr = new ArrayList<Tuple>();
        arr.add(new Tuple<String[], String>(new String[] { "James", "45" }, "Artist1"));
        arr.add(new Tuple<String[], String>(new String[] { "Roy", "70" }, "Artist2"));
        arr.add(new Tuple<String[], String>(new String[] { "Rushil", "50" }, "Artist3"));
        arr.add(new Tuple<String[], String>(new String[] { "Alan", "10" }, "Artist4"));
        arr.add(new Tuple<String[], String>(new String[] { "Jake", "90" }, "Artist5"));
        arr.add(new Tuple<String[], String>(new String[] { "Miley", "85" }, "Artist6"));

        // Collections.sort(arr, new Sort1(0));

    }
}
