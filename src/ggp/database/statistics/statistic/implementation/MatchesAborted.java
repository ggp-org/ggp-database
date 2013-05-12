package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.CounterStatistic;

import com.google.appengine.api.datastore.Entity;

public class MatchesAborted extends CounterStatistic {
    public void updateWithMatch(Entity newMatch) {
        if (!getProperty(newMatch, "isAborted", false)) return;
        incrementCounter(1.0);
    }
}