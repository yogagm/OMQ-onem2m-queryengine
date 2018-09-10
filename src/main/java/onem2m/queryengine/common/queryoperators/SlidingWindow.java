package onem2m.queryengine.common.queryoperators;

import org.apache.commons.math3.stat.descriptive.moment.Mean;

public class SlidingWindow {
    public double[] storage;
    int ct = 0;
    Mean mean = new Mean();


    public SlidingWindow(int size) {
        this.storage = new double[size];
    }

    public void put(double i) {
        storage[this.ct % storage.length] = i;
        this.ct++;
    }

    public double[] get() {
        return this.storage;
    }
}
