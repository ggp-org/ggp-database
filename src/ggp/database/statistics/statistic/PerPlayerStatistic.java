package ggp.database.statistics.statistic;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

public abstract class PerPlayerStatistic extends Statistic {
    private static final String playerPrefix = "player_";
    
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
    
    protected JSONObject getPerPlayerState(String playerName) {
        return getPerEntityState(playerPrefix, playerName);
    }
    
    protected void setPerPlayerVariable(String playerName, String varName, double toValue) {
        setVariable(getPerPlayerState(playerName), varName, toValue);
    }
    
    protected void incrementPerPlayerVariable(String playerName, String varName, double byValue) {
        incrementVariable(getPerPlayerState(playerName), varName, byValue);
    }
    
    public final Object getFinalForm() throws JSONException {
        JSONObject theFinalForm = new JSONObject();
        for (String playerName : getKnownPlayerNames()) {
            theFinalForm.put(playerName, getPerPlayerFinalForm(playerName));
        }
        return theFinalForm;
    }    

    @SuppressWarnings("unchecked")
    protected Set<String> getKnownPlayerNames() {
        Set<String> theKnownPlayers = new HashSet<String>();
        Iterator<String> i = getState().keys();
        while(i.hasNext()) {
            String theKey = i.next();
            if (theKey.startsWith(playerPrefix)) {
                theKnownPlayers.add(theKey.replaceFirst(playerPrefix, ""));
            }
        }
        return theKnownPlayers;
    }
    
    @SuppressWarnings("unchecked")
    public static List<String> getPlayerNames(Entity newMatch) {
        List<String> playerNames = new ArrayList<String>();
        if (newMatch.getProperty("playerNamesFromHost") == null) return playerNames;
        
        List<String> thePlayerNames = (List<String>)newMatch.getProperty("playerNamesFromHost");
        for (int i = 0; i < thePlayerNames.size(); i++) {
            playerNames.add(thePlayerNames.get(i));
        }
        return playerNames;
    }
    
    public abstract void updateWithMatch(Entity newMatch);    
    public abstract Object getPerPlayerFinalForm(String forPlayer) throws JSONException;
}