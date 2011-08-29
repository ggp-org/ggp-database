package ggp.database.statistics.statistic;

import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

public abstract class Statistic {
    private JSONObject state;
    
    public Statistic () {
        state = new JSONObject();
    }

    public abstract void updateWithMatch(JSONObject newMatch) throws JSONException;
    
    public abstract Object getFinalForm() throws JSONException;
    public abstract Object getPerGameFinalForm(String forGame) throws JSONException;
    public abstract Object getPerPlayerFinalForm(String forPlayer) throws JSONException;

    public void saveState(JSONObject toBeSerialized) {
        try {
            toBeSerialized.put(getClass().getSimpleName(), getState());
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public void loadState(JSONObject serializedForm) {
        try {
            state = serializedForm.getJSONObject(getClass().getSimpleName());
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    protected void setStateVariable(String varName, double toValue) {
        setVariable(getState(), varName, toValue);
    }    
    
    protected void incrementStateVariable(String varName, double byValue) {
        incrementVariable(getState(), varName, byValue);
    }
    
    protected JSONObject getState() {
        return state;
    }

    protected static void setVariable(JSONObject state, String varName, double toValue) {
        try {
            state.put(varName, toValue);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }    
    
    protected static void incrementVariable(JSONObject state, String varName, double byValue) {
        try {
            double value = byValue;
            if (!state.has(varName)) {
                value += state.getDouble(varName);
            }
            state.put(varName, value);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
}