package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.PerGameStatistic;
import ggp.database.statistics.statistic.WeightedAverageStatistic;

import com.google.appengine.api.datastore.Entity;

public class AverageMoves extends PerGameStatistic<WeightedAverageStatistic.NaiveWeightedAverage> {
    public void updateWithMatch(Entity newMatch) {
        if (!(Boolean)newMatch.getProperty("isCompleted")) return;

        String theGameURL = newMatch.getProperty("gameMetaURL").toString();
        Long nMoves = (Long)newMatch.getProperty("moveCount");
        getPerGameStatistic(theGameURL).addEntry(nMoves, 1.0);
    }

    @Override
    protected WeightedAverageStatistic.NaiveWeightedAverage getInitialStatistic() {
        return new WeightedAverageStatistic.NaiveWeightedAverage();
    }
}