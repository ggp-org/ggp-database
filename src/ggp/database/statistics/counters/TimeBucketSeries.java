package ggp.database.statistics.counters;

import java.util.ArrayList;
import java.util.List;

public class TimeBucketSeries {
    public static final long timeStart = 1240000000000L;
    public static final long timeInterval = 604800000L;
    public final long timeEnd = System.currentTimeMillis();
    public final int numBuckets = 1+(int)((timeEnd - timeStart)/timeInterval);

    public List<Integer> theBuckets;

    public TimeBucketSeries() {
        theBuckets = new ArrayList<Integer>();
        for (int i = 0; i < numBuckets; i++) {
            theBuckets.add(0);
        }
    }

    public void addEntry(long theEntryTime) {
        int nBucket = computeBucket(theEntryTime);
        theBuckets.set(nBucket, 1+theBuckets.get(nBucket));
    }

    public List<Integer> getTimeSeries() {
        return theBuckets;
    }

    private int computeBucket(long theTime) {
        if (theTime <= timeStart) return 0;
        if (theTime >= timeEnd) return numBuckets-1;
        return (int)((theTime - timeStart) / timeInterval);
    }

    /*
    private long getTimeForBucket(int nBucket) {
        if (nBucket <= 0) return timeStart;
        if (nBucket >= numBuckets-1) return timeEnd;
        return timeStart + timeInterval*nBucket;
    }
    */
}