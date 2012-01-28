package ggp.database.statistics.statistic;

import com.google.appengine.api.datastore.Entity;

public abstract class WinLossCounterStatistic extends Statistic {
    public WinLossCounterStatistic() {
        setStateVariable("wins", 0.0);
        setStateVariable("ties", 0.0);
        setStateVariable("losses", 0.0);        
    }

    protected void addEntry(int myScore, int theirScore) {
        if (myScore > theirScore) {
            incrementStateVariable("wins", 1);
        } else if (theirScore > myScore) {
            incrementStateVariable("losses", 1);
        } else {
            incrementStateVariable("ties", 1);
        }
    }

    public Object getFinalForm() {
        return "" + (int)getStateVariable("wins") + "-" + (int)getStateVariable("ties") + "-" + (int)getStateVariable("losses");
    }

    @Override
    public abstract void updateWithMatch(Entity newMatch);
    
    public static class NaiveWinLossCounter extends WinLossCounterStatistic {
        public void updateWithMatch(Entity newMatch) {};
        public void addEntry(int myScore, int theirScore) { super.addEntry(myScore, theirScore); }
    }    
}