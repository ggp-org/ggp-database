package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.PerGameStatistic;
import ggp.database.statistics.statistic.PerPlayerStatistic;
import ggp.database.statistics.statistic.WeightedAverageStatistic;

import java.util.List;

import com.google.appengine.api.datastore.Entity;

public class AverageScoreOn extends PerPlayerStatistic<PerGameStatistic<WeightedAverageStatistic.NaiveWeightedAverage>> {
    @SuppressWarnings("unchecked")
    public void updateWithMatch(Entity newMatch) {
        if (newMatch.getProperty("goalValues") == null) return;
        if (!(Boolean)newMatch.getProperty("isCompleted")) return;
        if (newMatch.getProperty("hashedMatchHostPK") == null) return;
        if ((Boolean)newMatch.getProperty("hasErrors")) return;

        String theGame = (String)newMatch.getProperty("gameMetaURL");
        
        List<String> playerNames = getPlayerNames(newMatch);
        if (playerNames == null) return;
        
        for (int i = 0; i < playerNames.size(); i++) {
            String aPlayerName = playerNames.get(i);
            int aPlayerScore = ((List<Long>)newMatch.getProperty("goalValues")).get(i).intValue();            
            getPerPlayerStatistic(aPlayerName).getPerGameStatistic(theGame).addEntry(aPlayerScore, 1.0);
        }
    }
    
    @Override
    public Object getFinalForm() {
        return null;
    }

    @Override
    protected PerGameStatistic<WeightedAverageStatistic.NaiveWeightedAverage> getInitialStatistic() {
        class X2 extends PerGameStatistic<WeightedAverageStatistic.NaiveWeightedAverage> {
            public void updateWithMatch(Entity newMatch) {}
            @Override protected WeightedAverageStatistic.NaiveWeightedAverage getInitialStatistic() {
                return new WeightedAverageStatistic.NaiveWeightedAverage();
            };
        }
        return new X2();
    }
}