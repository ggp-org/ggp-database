package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.CounterStatistic;
import ggp.database.statistics.statistic.PerTournamentStatistic;

import com.google.appengine.api.datastore.Entity;

public class LastTournamentUpdate extends PerTournamentStatistic<CounterStatistic.NaiveCounter> {
    public void updateWithMatch(Entity newMatch) {
        if (!(Boolean)newMatch.getProperty("isCompleted")) return;
        if (newMatch.getProperty("hashedMatchHostPK") == null) return;
        if (newMatch.getProperty("tournamentNameFromHost") == null) return;

        long theTime = (Long)newMatch.getProperty("startTime");
        String tournamentName = (String)newMatch.getProperty("tournamentNameFromHost");
        CounterStatistic.NaiveCounter theCounter = getPerTournamentStatistic(tournamentName);
        if (theCounter.getValue() < theTime) theCounter.setCounter(theTime);
    }

    @Override
    protected CounterStatistic.NaiveCounter getInitialStatistic() {
        return new CounterStatistic.NaiveCounter();
    }
}