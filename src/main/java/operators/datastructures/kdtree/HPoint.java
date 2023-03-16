package operators.datastructures.kdtree;

/**
 * 
 * based on work by Simon Levy
 * http://www.cs.wlu.edu/~levy/software/kd/
 */

import java.io.Serializable;

// Hyper-Point class supporting KDTree class

class HPoint implements Serializable{

    protected String[] coord;

    protected HPoint(int n) {
        coord = new String[n];
    }

    protected HPoint(String[] x) {
        coord = new String[x.length];
        for (int i = 0; i < x.length; ++i)
            coord[i] = x[i];
    }

    protected Object clone() {
        return new HPoint(coord);
    }

    protected boolean equals(HPoint p) {
        // seems faster than java.util.Arrays.equals(), which is not
        // currently supported by Matlab anyway
        for (int i = 0; i < coord.length; ++i)
            if (!coord[i].equals(p.coord[i]))
                return false;

        return true;
    }

    public String toString() {
        String s = "";
        for (int i = 0; i < coord.length; ++i) {
            s = s + coord[i] + " ";
        }
        return s;
    }
}
