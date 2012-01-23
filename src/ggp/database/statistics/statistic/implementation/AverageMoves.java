package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.CounterStatistic;
import ggp.database.statistics.statistic.PerGameStatistic;

import com.google.appengine.api.datastore.Entity;

public class AverageMoves extends PerGameStatistic<CounterStatistic.NaiveCounter> {
    public void updateWithMatch(Entity newMatch) {
        if (!(Boolean)newMatch.getProperty("isCompleted")) return;

        String theGameURL = newMatch.getProperty("gameMetaURL").toString();
        Long nMoves = (Long)newMatch.getProperty("moveCount");
        getPerGameStatistic(theGameURL).incrementCounter(nMoves);
    }

    @Override
    protected CounterStatistic.NaiveCounter getInitialStatistic() {
        return new CounterStatistic.NaiveCounter();
    }
}