package operators.datastructures;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Triplet;

import operators.datastructures.kdtree.KDTree;
import operators.datastructures.kdtree.QuickSelect;

class Sort implements Comparator<Object> {
    int dim;

    public Sort(int dim) {
        this.dim = dim;
    }

    @Override
    public int compare(Object a, Object b) {
        Pair<String[], String> p1 = (Pair<String[], String>) a;
        Pair<String[], String> p2 = (Pair<String[], String>) b;

        return (p1.getValue0())[this.dim].compareTo(p2.getValue0()[this.dim]);
    }
}

public class kd_exp {
    public static void main(String[] args)  {

        Object[] array = new Object[6];

        array[0] = new Pair<String[], String>(new String[] { "James", "45" }, "Artist1");
        array[1] = new Pair<String[], String>(new String[] { "Roy", "70" }, "Artist2");
        array[2] = new Pair<String[], String>(new String[] { "Rushil", "50" }, "Artist3");
        array[3] = new Pair<String[], String>(new String[] { "Alan", "10" }, "Artist4");
        array[4] = new Pair<String[], String>(new String[] { "Jake", "90" }, "Artist5");
        array[5] = new Pair<String[], String>(new String[] { "Miley", "85" }, "Artist6");

        // Arrays.sort(array, new Sort(1));

        ObjectInputStream objectInputStreamEdge;
        try {
            objectInputStreamEdge = new ObjectInputStream(new BufferedInputStream(new FileInputStream("src/test/java/Dataset/DBLP/DBLP_Small/KDTree/edge/unbalanced.ser")));
            HashMap<String, KDTree> KDTreeSetEdge = (HashMap<String, KDTree>) objectInputStreamEdge.readObject();

        } catch (Exception e) {
            StackTraceElement[] elements = e.getStackTrace();
            System.err.println(
                    "===================================== \n" + "[ERROR] lorem ipsum");
            for (int i = 0; i < 15; i++) {
                System.err.println(elements[i]);
            }
        }
    }

    public static KDTree medianKDTree(Object[] arr, int dims) {
        KDTree kdTree = new KDTree(dims);
        QuickSelect medianObj = new QuickSelect();

        Queue<Triplet<Integer, Integer, Integer>> queue = new LinkedList<>();
        queue.add(new Triplet<Integer, Integer, Integer>(0, arr.length - 1, 0));
        Triplet<Integer, Integer, Integer> cur_args;
        while (!queue.isEmpty()) {
            cur_args = queue.poll();
            Quartet<Integer, Integer, Integer, Object> quartet = medianObj.median(arr, cur_args.getValue0(),
                    cur_args.getValue1(), cur_args.getValue2() % dims);
            
            Pair<String[], String> p;
            try {
                p = (Pair<String[], String>) quartet.getValue3();
            } catch (Exception e) {
                continue;
            }
            kdTree.insert(p.getValue0(), p.getValue1());

            if (quartet.getValue0().compareTo(quartet.getValue2() - 1) <= 0) {
                queue.add(new Triplet<Integer, Integer, Integer>(quartet.getValue0(), quartet.getValue2() - 1,
                        cur_args.getValue2() + 1));
            }
            if (quartet.getValue1().compareTo(quartet.getValue2() + 1) >= 0) {
                queue.add(new Triplet<Integer, Integer, Integer>(quartet.getValue2() + 1, quartet.getValue1(),
                        cur_args.getValue2() + 1));
            }
        }
        return kdTree;

    }
}