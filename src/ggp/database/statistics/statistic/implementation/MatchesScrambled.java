package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.CounterStatistic;

import com.google.appengine.api.datastore.Entity;

public class MatchesScrambled extends CounterStatistic {
    public void updateWithMatch(Entity newMatch) {
    	if (!getProperty(newMatch, "scrambled", false)) return;
        incrementCounter(1.0);
    }
}