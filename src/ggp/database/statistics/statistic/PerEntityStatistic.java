package ggp.database.statistics.statistic;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.appengine.repackaged.org.json.JSONArray;
import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

public abstract class PerEntityStatistic extends Statistic {
    private static final String gamePrefix = "game_";
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
    
    protected JSONObject getPerGameState(String gameName) {
        return getPerEntityState(gamePrefix, gameName);
    }    
    
    protected void setPerPlayerVariable(String playerName, String varName, double toValue) {
        setVariable(getPerPlayerState(playerName), varName, toValue);
    }
    
    protected void incrementPerPlayerVariable(String playerName, String varName, double byValue) {
        incrementVariable(getPerPlayerState(playerName), varName, byValue);
    }
    
    protected void setPerGameVariable(String gameName, String varName, double toValue) {
        setVariable(getPerPlayerState(gameName), varName, toValue);
    }
    
    protected void incrementPerGameVariable(String gameName, String varName, double byValue) {
        incrementVariable(getPerPlayerState(gameName), varName, byValue);
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
    
    protected static List<String> getPlayerNames(JSONObject matchJSON) {
        if (!matchJSON.has("playerNamesFromHost"))
            return null;
        
        try {
            List<String> playerNames = new ArrayList<String>();
            JSONArray thePlayerNames = matchJSON.getJSONArray("playerNamesFromHost");
            for (int i = 0; i < thePlayerNames.length(); i++) {
                playerNames.add(thePlayerNames.getString(i));
            }
            return playerNames;
        } catch (JSONException je) {
            throw new RuntimeException(je);
        }
    }
    
    public abstract void updateWithMatch(JSONObject newMatch) throws JSONException;
    
    public abstract Object getFinalForm() throws JSONException;
    public abstract Object getPerGameFinalForm(String forGame) throws JSONException;
    public abstract Object getPerPlayerFinalForm(String forPlayer) throws JSONException;
}