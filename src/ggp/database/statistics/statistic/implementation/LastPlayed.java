package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.CounterStatistic;
import ggp.database.statistics.statistic.PerPlayerStatistic;

import java.util.List;

import com.google.appengine.api.datastore.Entity;

public class LastPlayed extends PerPlayerStatistic<CounterStatistic.NaiveCounter> {
    public void updateWithMatch(Entity newMatch) {
        if (!(Boolean)newMatch.getProperty("isCompleted")) return;
        if (newMatch.getProperty("hashedMatchHostPK") == null) return;
        if ((Boolean)newMatch.getProperty("hasErrors")) return;

        List<String> playerNames = getPlayerNames(newMatch);
        if (playerNames == null) return;

        for (int i = 0; i < playerNames.size(); i++) {            
            long theTime = (Long)newMatch.getProperty("startTime");
            CounterStatistic.NaiveCounter theCounter = getPerPlayerStatistic(playerNames.get(i));
            if (theCounter.getValue() < theTime) theCounter.setCounter(theTime);
        }
    }

    @Override
    protected CounterStatistic.NaiveCounter getInitialStatistic() {
        return new CounterStatistic.NaiveCounter();
    }
}