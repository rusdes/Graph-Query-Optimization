package operators.datastructures;
import java.util.Arrays;

import operators.datastructures.kdtree.KDTree;

public class kd_exp {
    public static void main(String[] args) {
        KDTree kdTree = new KDTree(2); //2 dimensions (x, y)
        // point insertion:
        kdTree.insert(new double[]{40, 45}, "Artist1");
        kdTree.insert(new double[]{15, 70}, "Artist2");
        kdTree.insert(new double[]{69, 50}, "Artist3");
        kdTree.insert(new double[]{70, 10}, "Artist4");
        kdTree.insert(new double[]{85, 90}, "Artist5");
        kdTree.insert(new double[]{66, 85}, "Artist6");

        
        Object[] output = kdTree.range(new double[]{Double.NEGATIVE_INFINITY, 50}, new double[]{Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY});
        System.out.println("Output = " + Arrays.toString(output));
        System.out.println(kdTree.toString()); //check the data
    }
}
