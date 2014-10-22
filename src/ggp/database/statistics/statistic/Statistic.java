package ggp.database.statistics.statistic;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.appengine.api.datastore.Entity;

import external.JSON.JSONException;
import external.JSON.JSONObject;

public abstract class Statistic {
    private JSONObject state;
    
    public Statistic () {
        state = new JSONObject();
    }
    
    public void setState(JSONObject x) {
        state = x;
    }
    
    public static interface Reader {
        public <T extends Statistic> T getStatistic(Class<T> c);
    }
    
    public static class FinalForm {
        public String name;
        public Object value;
        public FinalForm(String name, Object value) {
            this.name = name;
            this.value = value;
        }
    }

    // OVERRIDE: for updating the stats
    public abstract void updateWithMatch(Entity newMatch);
    public void finalizeComputation(Reader theReader) {};

    // OVERRIDE: for returning the final stats
    protected abstract Object getFinalForm() throws JSONException;
    protected Object getPerGameFinalForm(String forGame) throws JSONException { return null; }
    protected Object getPerPlayerFinalForm(String forPlayer) throws JSONException { return null; }    

    // OVERRIDE only if you need to return multiple forms for a single statistic
    public Set<FinalForm> getFinalForms() throws JSONException { return new HashSet<FinalForm>(Arrays.asList(new FinalForm[] {new FinalForm(camelCase(getClass().getSimpleName()), getFinalForm())})); }
    public Set<FinalForm> getPerGameFinalForms(String forGame) throws JSONException { return new HashSet<FinalForm>(Arrays.asList(new FinalForm[] {new FinalForm(camelCase(getClass().getSimpleName()), getPerGameFinalForm(forGame))})); }
    public Set<FinalForm> getPerPlayerFinalForms(String forPlayer) throws JSONException { return new HashSet<FinalForm>(Arrays.asList(new FinalForm[] {new FinalForm(camelCase(getClass().getSimpleName()), getPerPlayerFinalForm(forPlayer))})); }
    
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
            ;
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
    
    public static String camelCase(String x) {
        return x.substring(0,1).toLowerCase() + x.substring(1);
    }
    
    @SuppressWarnings("unchecked")
    public static <T> T getProperty(Entity entity, String propertyName, T defaultValue) {
    	if (!entity.hasProperty(propertyName)) return defaultValue;
    	if (entity.getProperty(propertyName) == null) return defaultValue;    	
    	return (T)entity.getProperty(propertyName);
    }
}