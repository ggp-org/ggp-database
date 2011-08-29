package ggp.database.statistics.counters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TimeBucketSeries {
    public static final int NUM_BUCKETS = 100;
    public static final long timeStart = 1240000000000L;
    public final long timeEnd = System.currentTimeMillis();
    
    public List<Integer> theBuckets;
    
    public TimeBucketSeries() {
        theBuckets = new ArrayList<Integer>();
        for (int i = 0; i < NUM_BUCKETS; i++) {
            theBuckets.add(0);
        }
    }
    
    public void addEntry(long theEntryTime) {
        int nBucket = computeBucket(theEntryTime);
        theBuckets.set(nBucket, 1+theBuckets.get(nBucket));
    }
    
    public Map<Long,Integer> getTimeMap() {
        Map<Long,Integer> x = new HashMap<Long,Integer>();
        for (int i = 0; i < NUM_BUCKETS; i++) {
            x.put(getTimeForBucket(i), theBuckets.get(i));
        }
        return x;
    }
    
    private int computeBucket(long theTime) {
        if (theTime <= timeStart) return 0;
        if (theTime >= timeEnd) return NUM_BUCKETS-1;
        long theTimeInterval = (timeEnd - timeStart)/NUM_BUCKETS;
        return (int)((theTime - timeStart) / theTimeInterval);
    }
    
    private long getTimeForBucket(int nBucket) {
        if (nBucket <= 0) return timeStart;
        if (nBucket >= NUM_BUCKETS-1) return timeEnd;
        long theTimeInterval = (timeEnd - timeStart)/NUM_BUCKETS;
        return timeStart + theTimeInterval*nBucket;
    }
}