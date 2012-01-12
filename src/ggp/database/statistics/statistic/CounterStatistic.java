package ggp.database.statistics.statistic;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.repackaged.org.json.JSONException;

public abstract class CounterStatistic extends Statistic {
    public CounterStatistic () {
        super();
        setStateVariable("value", 0.0);
    }
    
    protected void setCounter(double toValue) {
        setStateVariable("value", toValue);
    }
    
    protected void incrementCounter(double byValue) {
        incrementStateVariable("value", byValue);
    }
    
    public Object getFinalForm() throws JSONException {
        return getState().getDouble("value");
    }
    public Object getPerGameFinalForm(String forGame) { return null; }
    public Object getPerPlayerFinalForm(String forPlayer) { return null; }
    
    @Override
    public abstract void updateWithMatch(Entity newMatch);    
}