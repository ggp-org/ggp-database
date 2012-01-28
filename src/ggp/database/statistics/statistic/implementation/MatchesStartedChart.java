package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.TimeSeriesStatistic;

import com.google.appengine.api.datastore.Entity;

public class MatchesStartedChart extends TimeSeriesStatistic {
    public void updateWithMatch(Entity newMatch) {
        if (!(Boolean)newMatch.getProperty("isCompleted")) return;
        addEntry((Long)newMatch.getProperty("startTime"));
    }
}