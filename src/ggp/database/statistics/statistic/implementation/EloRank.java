package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.CounterStatistic;
import ggp.database.statistics.statistic.PerPlayerStatistic;

import java.util.List;

import com.google.appengine.api.datastore.Entity;

public class EloRank extends PerPlayerStatistic<CounterStatistic.NaiveCounter> {
    @SuppressWarnings("unchecked")
    public void updateWithMatch(Entity newMatch) {
        if (newMatch.getProperty("goalValues") == null) return;        
        if (!(Boolean)newMatch.getProperty("isCompleted")) return;
        if (newMatch.getProperty("hashedMatchHostPK") == null) return;
        if ((Boolean)newMatch.getProperty("hasErrors")) return;

        List<String> playerNames = getPlayerNames(newMatch);
        if (playerNames == null) return;

        for (int i = 0; i < playerNames.size(); i++) {
            CounterStatistic.NaiveCounter aPlayer = getPerPlayerStatistic(playerNames.get(i));
            double aPlayerScore = ((List<Long>)newMatch.getProperty("goalValues")).get(i);
            updateMatchParticipation(aPlayer, newMatch.getProperty("gameMetaURL").toString(), i, aPlayerScore);
            for (int j = i+1; j < playerNames.size(); j++) {
                CounterStatistic.NaiveCounter bPlayer = getPerPlayerStatistic(playerNames.get(j));                
                double bPlayerScore = ((List<Long>)newMatch.getProperty("goalValues")).get(j);
                updateRank(aPlayer, bPlayer, aPlayerScore, bPlayerScore);
            }
        }
    }
    
    @Override
    protected CounterStatistic.NaiveCounter getInitialStatistic() {
        return new CounterStatistic.NaiveCounter();
    }
    
    /* Private functions */
    protected void updateMatchParticipation(CounterStatistic.NaiveCounter aPlayer, String theGame, int theRole, double aScore) {
        ;
    }    
    
    protected void updateRank(CounterStatistic.NaiveCounter aPlayer, CounterStatistic.NaiveCounter bPlayer, double aPlayerScore, double bPlayerScore) {
        if (aPlayerScore + bPlayerScore != 100) return;
        
        double EA = getExpectedScore(aPlayer, bPlayer);
        double EB = 1.0 - EA;
        
        aPlayer.incrementCounter(aPlayerScore/100.0 - EA);
        bPlayer.incrementCounter(bPlayerScore/100.0 - EB);
    }
    
    private double getExpectedScore(CounterStatistic.NaiveCounter thePlayer, CounterStatistic.NaiveCounter opposingPlayer) {
        double RA = thePlayer.getValue();
        double RB = opposingPlayer.getValue();
        double QA = Math.pow(10.0, RA / 400.0);
        double QB = Math.pow(10.0, RB / 400.0);
        return QA / (QA + QB);
    }
}