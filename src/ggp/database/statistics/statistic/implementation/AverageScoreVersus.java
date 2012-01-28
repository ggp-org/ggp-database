package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.PerPlayerStatistic;
import ggp.database.statistics.statistic.WeightedAverageStatistic;

import java.util.List;

import com.google.appengine.api.datastore.Entity;

public class AverageScoreVersus extends PerPlayerStatistic<PerPlayerStatistic<WeightedAverageStatistic.NaiveWeightedAverage>> {
    @SuppressWarnings("unchecked")
    public void updateWithMatch(Entity newMatch) {
        if (newMatch.getProperty("goalValues") == null) return;
        if (!(Boolean)newMatch.getProperty("isCompleted")) return;
        if (newMatch.getProperty("hashedMatchHostPK") == null) return;
        if ((Boolean)newMatch.getProperty("hasErrors")) return;

        List<String> playerNames = getPlayerNames(newMatch);
        if (playerNames == null) return;
        
        for (int i = 0; i < playerNames.size(); i++) {
            String aPlayerName = playerNames.get(i);
            int aPlayerScore = ((List<Long>)newMatch.getProperty("goalValues")).get(i).intValue();
            for (int j = i+1; j < playerNames.size(); j++) {                
                String bPlayerName = playerNames.get(j);
                int bPlayerScore = ((List<Long>)newMatch.getProperty("goalValues")).get(j).intValue();
                
                getPerPlayerStatistic(aPlayerName).getPerPlayerStatistic(bPlayerName).addEntry(aPlayerScore, 1.0);
                getPerPlayerStatistic(bPlayerName).getPerPlayerStatistic(aPlayerName).addEntry(bPlayerScore, 1.0);
            }
        }
    }
    
    @Override
    public Object getFinalForm() {
        return null;
    }

    @Override
    protected PerPlayerStatistic<WeightedAverageStatistic.NaiveWeightedAverage> getInitialStatistic() {
        class X2 extends PerPlayerStatistic<WeightedAverageStatistic.NaiveWeightedAverage> {
            public void updateWithMatch(Entity newMatch) {}
            @Override protected WeightedAverageStatistic.NaiveWeightedAverage getInitialStatistic() {
                return new WeightedAverageStatistic.NaiveWeightedAverage();
            };
        }
        return new X2();
    }
}