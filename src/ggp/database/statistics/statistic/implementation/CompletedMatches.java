package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.CounterStatistic;

import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

public class CompletedMatches extends CounterStatistic {
    public void updateWithMatch(JSONObject newMatch) throws JSONException {
        incrementCounter(1.0);
    }
}