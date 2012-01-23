package ggp.database.statistics.statistic;

import com.google.appengine.api.datastore.Entity;

public abstract class CounterStatistic extends Statistic {
    public CounterStatistic() {
        setStateVariable("value", 0.0);
    }
    
    protected void setCounter(double toValue) {
        setStateVariable("value", toValue);
    }
    
    protected void incrementCounter(double byValue) {
        incrementStateVariable("value", byValue);
    }
    
    protected double getValue() {
        return getStateVariable("value");
    }
    
    public Object getFinalForm() {
        return getValue();
    }
    
    @Override
    public abstract void updateWithMatch(Entity newMatch);
    
    public static class NaiveCounter extends CounterStatistic {
        public void updateWithMatch(Entity newMatch) {};
        public void incrementCounter(double byValue) { super.incrementCounter(byValue); }
        public double getValue() { return super.getValue(); }
    }
}