package ggp.database.statistics.statistic;

import java.util.ArrayList;
import java.util.List;

import com.google.appengine.api.datastore.Entity;
import external.JSON.JSONException;
import external.JSON.JSONArray;

public abstract class TimeSeriesStatistic extends Statistic {
    public static final long timeStart = 1240000000000L;
    public static final long timeInterval = 604800000L;    
    
    public TimeSeriesStatistic() {        
        long timeEnd = System.currentTimeMillis();
        int numBuckets = 1+(int)((timeEnd - timeStart)/timeInterval);        
        List<Integer> theBuckets = new ArrayList<Integer>();
        for (int i = 0; i < numBuckets; i++) {
            theBuckets.add(0);
        }
        try {
            getState().put("timeEnd", timeEnd);
            getState().put("numBuckets", numBuckets);
            getState().put("theBuckets", theBuckets);
        } catch (JSONException e) {
            ;
        }
    }
    
    protected void addEntry(long theTime) {
        double timeEnd = getStateVariable("timeEnd");
        int numBuckets = (int)getStateVariable("numBuckets");
        
        int nBucket = 0;
        if (theTime >= timeEnd) {
            nBucket = numBuckets-1;
        } else if (theTime > timeStart) {
            nBucket = (int)((theTime - timeStart) / timeInterval);        
        }
        
        try {
            JSONArray theBuckets = getState().getJSONArray("theBuckets");
            theBuckets.put(nBucket, 1 + theBuckets.getInt(nBucket));
        } catch (JSONException e) {
            ;
        }
    }
    
    public Object getFinalForm() {
        try {
            JSONArray theBuckets = getState().getJSONArray("theBuckets");
            List<Integer> theTimeSeries = new ArrayList<Integer>();
            for (int i = 0; i < theBuckets.length(); i++) {
                theTimeSeries.add(theBuckets.getInt(i));
            }
            return theTimeSeries;
        } catch (JSONException e) {
            ;
        }
        return null;
    }
    
    @Override
    public abstract void updateWithMatch(Entity newMatch);
}