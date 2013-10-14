package ggp.database.statistics.statistic.implementation;

import java.util.List;
import com.google.appengine.api.datastore.Entity;
import ggp.database.statistics.statistic.WeightedAverageStatistic;
import ggp.database.statistics.statistic.PerPlayerStatistic;

public class ErrorRateForPlayer extends PerPlayerStatistic<WeightedAverageStatistic.NaiveWeightedAverage> {
	@Override
	public void updateWithMatch(Entity newMatch) {
        if (!(Boolean)newMatch.getProperty("isCompleted")) return;        
        @SuppressWarnings("unchecked")
        List<Boolean> hasErrorsForPlayer = (List<Boolean>)newMatch.getProperty("hasErrorsForPlayer");
        List<String> playerNames = getPlayerNames(newMatch);
        if (playerNames == null || hasErrorsForPlayer == null) return;
        
        for (int i = 0; i < playerNames.size(); i++) {
            String aPlayerName = playerNames.get(i);            
            getPerPlayerStatistic(aPlayerName).addEntry(hasErrorsForPlayer.get(i) ? 1 : 0, 1.0);
        }
	}

	@Override
	protected WeightedAverageStatistic.NaiveWeightedAverage getInitialStatistic() {
		return new WeightedAverageStatistic.NaiveWeightedAverage();
	}
}