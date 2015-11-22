package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.CounterStatistic;
import ggp.database.statistics.statistic.PerTournamentStatistic;

import com.google.appengine.api.datastore.Entity;

public class TournamentMatchesCompleted extends PerTournamentStatistic<CounterStatistic.NaiveCounter> {
    public void updateWithMatch(Entity newMatch) {
        if (newMatch.getProperty("goalValues") == null) return;        
        if (!(Boolean)newMatch.getProperty("isCompleted")) return;
        if (newMatch.getProperty("hashedMatchHostPK") == null) return;
        if (newMatch.getProperty("tournamentNameFromHost") == null) return;

        String tournamentName = (String)newMatch.getProperty("tournamentNameFromHost");
        CounterStatistic.NaiveCounter theCounter = getPerTournamentStatistic(tournamentName);
        theCounter.incrementCounter(1);
    }
	@Override
	protected CounterStatistic.NaiveCounter getInitialStatistic() {
		return new CounterStatistic.NaiveCounter();
	}
}