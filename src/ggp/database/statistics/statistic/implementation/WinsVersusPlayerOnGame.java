package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.PerGameStatistic;
import ggp.database.statistics.statistic.PerPlayerStatistic;
import ggp.database.statistics.statistic.WinLossCounterStatistic;
import ggp.database.statistics.statistic.WinLossCounterStatistic.NaiveWinLossCounter;

import java.util.List;

import com.google.appengine.api.datastore.Entity;

public class WinsVersusPlayerOnGame extends PerPlayerStatistic<PerPlayerStatistic<PerGameStatistic<WinLossCounterStatistic.NaiveWinLossCounter>>> {
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
            for (int j = i+1; j < playerNames.size(); j++) {                
                String bPlayerName = playerNames.get(j);
                int bPlayerScore = ((List<Long>)newMatch.getProperty("goalValues")).get(j).intValue();
                
                getPerPlayerStatistic(aPlayerName).getPerPlayerStatistic(bPlayerName).getPerGameStatistic(theGame).addEntry(aPlayerScore, bPlayerScore);
                getPerPlayerStatistic(bPlayerName).getPerPlayerStatistic(aPlayerName).getPerGameStatistic(theGame).addEntry(bPlayerScore, aPlayerScore);
            }
        }
    }
    
    @Override
    public Object getFinalForm() {
        return null;
    }

    @Override
    protected PerPlayerStatistic<PerGameStatistic<WinLossCounterStatistic.NaiveWinLossCounter>> getInitialStatistic() {
        class X1 extends PerPlayerStatistic<PerGameStatistic<WinLossCounterStatistic.NaiveWinLossCounter>> {
            class X2 extends PerGameStatistic<WinLossCounterStatistic.NaiveWinLossCounter> {
                public void updateWithMatch(Entity newMatch) {}
                @Override protected WinLossCounterStatistic.NaiveWinLossCounter getInitialStatistic() {
                    return new WinLossCounterStatistic.NaiveWinLossCounter();
                };
            }
            
            public void updateWithMatch(Entity newMatch) {}
            @Override protected PerGameStatistic<NaiveWinLossCounter> getInitialStatistic() {
                return new X2();
            };
        }        
        return new X1();
    }
}