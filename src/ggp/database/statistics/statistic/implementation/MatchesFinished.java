package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.CounterStatistic;

import com.google.appengine.api.datastore.Entity;

public class MatchesFinished extends CounterStatistic {
    public void updateWithMatch(Entity newMatch) {
        if (!(Boolean)newMatch.getProperty("isCompleted")) return;
        incrementCounter(1.0);
    }
}