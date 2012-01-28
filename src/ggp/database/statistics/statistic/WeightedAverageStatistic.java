package ggp.database.statistics.statistic;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.repackaged.org.json.JSONArray;
import com.google.appengine.repackaged.org.json.JSONException;

public abstract class WeightedAverageStatistic extends Statistic {
    public WeightedAverageStatistic() {
        setStateVariable("totalValue", 0.0);
        setStateVariable("totalWeight", 0.0);
        setStateVariable("totalEntries", 0.0);
    }
    
    protected void addEntry(double value, double weight) {
        incrementStateVariable("totalValue", value*weight);
        incrementStateVariable("totalWeight", weight);
        incrementStateVariable("totalEntries", 1.0);
    }
    
    public double getWeightedAverage() {
        try {
            if (getState().getDouble("totalWeight") != 0) {
                return getState().getDouble("totalValue") / getState().getDouble("totalWeight");
            } else {
                return 0;
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public Object getFinalForm() throws JSONException {
        JSONArray theFinalForm = new JSONArray();
        theFinalForm.put(getWeightedAverage());
        theFinalForm.put(getState().getDouble("totalEntries"));
        return theFinalForm;
    }

    @Override
    public abstract void updateWithMatch(Entity newMatch);
    
    public static class NaiveWeightedAverage extends WeightedAverageStatistic {
        public void updateWithMatch(Entity newMatch) {};
        public void addEntry(double value, double weight) { super.addEntry(value, weight); }
    }    
}