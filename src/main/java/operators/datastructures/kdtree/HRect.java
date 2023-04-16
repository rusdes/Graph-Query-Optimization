package operators.datastructures.kdtree;

import java.io.Serializable;

/**
 * %SVN.HEADER%
 * 
 * based on work by Simon Levy
 * http://www.cs.wlu.edu/~levy/software/kd/
 */

// Hyper-Rectangle class supporting KDTree class

class HRect implements Serializable {

    protected HPoint min;
    protected HPoint max;

    protected HRect(int ndims) {
        min = new HPoint(ndims);
        max = new HPoint(ndims);
    }

    protected HRect(HPoint vmin, HPoint vmax) {

        min = (HPoint) vmin.clone();
        max = (HPoint) vmax.clone();
    }

    protected Object clone() {

        return new HRect(min, max);
    }

    // from Moore's eqn. 6.6
    protected HPoint closest(HPoint t) {
        HPoint p = new HPoint(t.coord.length);

        for (int i = 0; i < t.coord.length; ++i) {
            if (t.coord[i].compareTo(min.coord[i]) <= 0) {
                p.coord[i] = min.coord[i];
            } else if (t.coord[i].compareTo(max.coord[i]) >= 0) {
                p.coord[i] = max.coord[i];
            } else {
                p.coord[i] = t.coord[i];
            }
        }

        return p;
    }

    // used in initial conditions of KDTree.nearest()
    protected static HRect infiniteHRect(int d) {

        HPoint vmin = new HPoint(d);
        HPoint vmax = new HPoint(d);

        for (int i = 0; i < d; ++i) {
            vmin.coord[i] = Character.toString(Character.MIN_VALUE);
            vmax.coord[i] = Character.toString(Character.MAX_VALUE);
        }

        return new HRect(vmin, vmax);
    }

    public String toString() {
        return min + "\n" + max + "\n";
    }
}