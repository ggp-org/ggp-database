package ggp.database.statistics.statistic;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

public abstract class Statistic {
    private JSONObject state;
    
    public Statistic () {
        state = new JSONObject();
    }
    
    public interface Reader {
        public <T extends Statistic> T getStatistic(Class<T> c);
    }

    // OVERRIDE: for updating the stats
    public abstract void updateWithMatch(Entity newMatch);
    public void finalizeComputation(Reader theReader) {};
    
    // OVERRIDE: for returning the final stats
    public abstract Object getFinalForm() throws JSONException;
    public Object getPerGameFinalForm(String forGame) throws JSONException { return null; }
    public Object getPerPlayerFinalForm(String forPlayer) throws JSONException { return null; }
    
    // ==== Utility functions ====
    
    public final void saveState(JSONObject toBeSerialized) {
        try {
            toBeSerialized.put(getClass().getSimpleName(), getState());
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public final void loadState(JSONObject serializedForm) {
        try {
            state = serializedForm.getJSONObject(getClass().getSimpleName());
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    protected final double getStateVariable(String varName) {
        return getVariable(getState(), varName);
    }
    
    protected final void setStateVariable(String varName, double toValue) {
        setVariable(getState(), varName, toValue);
    }    
    
    protected final void incrementStateVariable(String varName, double byValue) {
        incrementVariable(getState(), varName, byValue);
    }
    
    protected final JSONObject getState() {
        return state;
    }

    protected static final double getVariable(JSONObject state, String varName) {
        try {
            return state.getDouble(varName);
        } catch (JSONException e) {
            return 0;
        }
    }
    
    protected static final void setVariable(JSONObject state, String varName, double toValue) {
        try {
            state.put(varName, toValue);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
    
    protected static final void incrementVariable(JSONObject state, String varName, double byValue) {
        try {
            double value = byValue;
            if (state.has(varName)) {
                value += state.getDouble(varName);
            }
            state.put(varName, value);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
}