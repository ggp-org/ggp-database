package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.WeightedAverageStatistic;

import com.google.appengine.api.datastore.Entity;

public class MatchesAveragePlayers extends WeightedAverageStatistic {
    public void updateWithMatch(Entity newMatch) {
        if (newMatch.getProperty("matchRoles") == null) return;
        addEntry((Long)newMatch.getProperty("matchRoles"), 1.0);
    }
}