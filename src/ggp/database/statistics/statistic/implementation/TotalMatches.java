package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.CounterStatistic;

import com.google.appengine.api.datastore.Entity;

public class TotalMatches extends CounterStatistic {
    public void updateWithMatch(Entity newMatch) {
        incrementCounter(1.0);
    }
}