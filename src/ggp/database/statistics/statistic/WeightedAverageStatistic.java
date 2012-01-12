package ggp.database.statistics.statistic;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.repackaged.org.json.JSONArray;
import com.google.appengine.repackaged.org.json.JSONException;

public abstract class WeightedAverageStatistic extends Statistic {
    public WeightedAverageStatistic () {
        super();
        setStateVariable("totalValue", 0.0);
        setStateVariable("totalWeight", 0.0);
        setStateVariable("totalEntries", 0.0);
    }

    protected void addEntry(double value, double weight) {
        incrementStateVariable("totalValue", value);
        incrementStateVariable("totalWeight", weight);
        incrementStateVariable("totalEntries", 1.0);
    }

    public Object getFinalForm() throws JSONException {
        JSONArray theFinalForm = new JSONArray();
        theFinalForm.put(getState().getDouble("totalValue") / getState().getDouble("totalWeight"));
        theFinalForm.put(getState().getDouble("totalEntries"));
        return theFinalForm;
    }
    public Object getPerGameFinalForm(String forGame) { return null; }
    public Object getPerPlayerFinalForm(String forPlayer) { return null; }

    @Override
    public abstract void updateWithMatch(Entity newMatch);    
}