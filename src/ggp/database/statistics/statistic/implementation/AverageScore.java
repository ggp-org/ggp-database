package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.PerPlayerStatistic;
import ggp.database.statistics.statistic.WeightedAverageStatistic;

import java.util.List;

import com.google.appengine.api.datastore.Entity;

public class AverageScore extends PerPlayerStatistic<WeightedAverageStatistic.NaiveWeightedAverage> {
    @SuppressWarnings("unchecked")
    public void updateWithMatch(Entity newMatch) {
        if (newMatch.getProperty("goalValues") == null) return;        
        if (!(Boolean)newMatch.getProperty("isCompleted")) return;
        if (newMatch.getProperty("hashedMatchHostPK") == null) return;
        if ((Boolean)newMatch.getProperty("hasErrors")) return;

        List<String> playerNames = getPlayerNames(newMatch);
        if (playerNames == null) return;

        for (int i = 0; i < playerNames.size(); i++) {
            double theScore = ((List<Long>)newMatch.getProperty("goalValues")).get(i);
            getPerPlayerStatistic(playerNames.get(i)).addEntry(theScore, 1);
        }
    }

    @Override
    protected WeightedAverageStatistic.NaiveWeightedAverage getInitialStatistic() {
        return new WeightedAverageStatistic.NaiveWeightedAverage();
    }
}