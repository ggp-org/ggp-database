package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.WeightedAverageStatistic;

import com.google.appengine.api.datastore.Entity;

public class MatchesAverageMoves extends WeightedAverageStatistic {
    public void updateWithMatch(Entity newMatch) {
        if (newMatch.getProperty("moveCount") == null) return;
        addEntry((Long)newMatch.getProperty("moveCount"), 1.0);
    }
}