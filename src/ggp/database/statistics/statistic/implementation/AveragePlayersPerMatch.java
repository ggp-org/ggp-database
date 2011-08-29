package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.WeightedAverageStatistic;

import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

public class AveragePlayersPerMatch extends WeightedAverageStatistic {
    public void updateWithMatch(JSONObject newMatch) throws JSONException {
        addEntry(newMatch.getJSONArray("moves").getJSONArray(0).length(), 1.0);
    }
}