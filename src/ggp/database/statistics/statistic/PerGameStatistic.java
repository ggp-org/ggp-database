package ggp.database.statistics.statistic;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

public abstract class PerGameStatistic extends Statistic {
    private static final String gamePrefix = "game_";
    
    private JSONObject getPerEntityState(String thePrefix, String theName) {
        try {
            if(!getState().has(thePrefix + theName)) {                
                getState().put(thePrefix + theName, new JSONObject());
            }
            return getState().getJSONObject(thePrefix + theName);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
    
    protected JSONObject getPerGameState(String gameName) {
        return getPerEntityState(gamePrefix, gameName);
    }    
    
    protected void setPerGameVariable(String gameName, String varName, double toValue) {
        setVariable(getPerGameState(gameName), varName, toValue);
    }
    
    protected void incrementPerGameVariable(String gameName, String varName, double byValue) {
        incrementVariable(getPerGameState(gameName), varName, byValue);
    }
    
    public final Object getFinalForm() throws JSONException {
        JSONObject theFinalForm = new JSONObject();
        for (String gameName : getKnownGameNames()) {
            theFinalForm.put(gameName, getPerGameFinalForm(gameName));
        }
        return theFinalForm;
    }
    
    @SuppressWarnings("unchecked")
    protected Set<String> getKnownGameNames() {
        Set<String> theKnownPlayers = new HashSet<String>();
        Iterator<String> i = getState().keys();
        while(i.hasNext()) {
            String theKey = i.next();
            if (theKey.startsWith(gamePrefix)) {
                theKnownPlayers.add(theKey.replaceFirst(gamePrefix, ""));
            }
        }
        return theKnownPlayers;
    }

    public abstract void updateWithMatch(JSONObject newMatch) throws JSONException;
    public abstract Object getPerGameFinalForm(String forGame) throws JSONException;
}