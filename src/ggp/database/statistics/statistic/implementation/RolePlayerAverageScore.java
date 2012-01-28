package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.PerGameStatistic;
import ggp.database.statistics.statistic.PerPlayerStatistic;
import ggp.database.statistics.statistic.PerRoleStatistic;
import ggp.database.statistics.statistic.WeightedAverageStatistic;

import java.util.List;

import com.google.appengine.api.datastore.Entity;

public class RolePlayerAverageScore extends PerGameStatistic<PerRoleStatistic<PerPlayerStatistic<WeightedAverageStatistic.NaiveWeightedAverage>>> {
    @SuppressWarnings("unchecked")
    public void updateWithMatch(Entity newMatch) {
        if (newMatch.getProperty("goalValues") == null) return;
        if (!(Boolean)newMatch.getProperty("isCompleted")) return;
        if (newMatch.getProperty("hashedMatchHostPK") == null) return;
        if ((Boolean)newMatch.getProperty("hasErrors")) return;

        String theGame = (String)newMatch.getProperty("gameMetaURL");
        
        List<String> playerNames = PerPlayerStatistic.getPlayerNames(newMatch);
        if (playerNames == null) return;
        
        for (int i = 0; i < playerNames.size(); i++) {
            String aPlayerName = playerNames.get(i);
            int aPlayerScore = ((List<Long>)newMatch.getProperty("goalValues")).get(i).intValue();            
            getPerGameStatistic(theGame).getPerRoleStatistic(i).getPerPlayerStatistic(aPlayerName).addEntry(aPlayerScore, 1.0);
        }
    }
    
    @Override
    public Object getFinalForm() {
        return null;
    }

    @Override
    protected PerRoleStatistic<PerPlayerStatistic<WeightedAverageStatistic.NaiveWeightedAverage>> getInitialStatistic() {
        class X1 extends PerRoleStatistic<PerPlayerStatistic<WeightedAverageStatistic.NaiveWeightedAverage>> {
            class X2 extends PerPlayerStatistic<WeightedAverageStatistic.NaiveWeightedAverage> {
                public void updateWithMatch(Entity newMatch) {}
                @Override protected WeightedAverageStatistic.NaiveWeightedAverage getInitialStatistic() {
                    return new WeightedAverageStatistic.NaiveWeightedAverage();
                };
            }

            public void updateWithMatch(Entity newMatch) {}
            @Override protected PerPlayerStatistic<WeightedAverageStatistic.NaiveWeightedAverage> getInitialStatistic() {
                return new X2();
            };
        }
        return new X1();
    }
}