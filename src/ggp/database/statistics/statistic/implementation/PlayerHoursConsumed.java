package ggp.database.statistics.statistic.implementation;

import java.util.List;

import ggp.database.statistics.statistic.CounterStatistic;
import ggp.database.statistics.statistic.PerPlayerStatistic;

import com.google.appengine.api.datastore.Entity;

public class PlayerHoursConsumed extends CounterStatistic {
	private static final double MILLIS_PER_HOUR = 60*60*1000;
    public void updateWithMatch(Entity newMatch) {
    	int nRealPlayersForMatch = 0;
        List<String> theMatchPlayers = PerPlayerStatistic.getPlayerNames(newMatch);
        for (String playerName : theMatchPlayers) {
        	nRealPlayersForMatch += playerName.toLowerCase().equals("random") ? 0 : 1;
        }
        long millisecondsConsumed = (Long)newMatch.getProperty("matchLength") * nRealPlayersForMatch;
        incrementCounter(millisecondsConsumed/MILLIS_PER_HOUR);
    }
}