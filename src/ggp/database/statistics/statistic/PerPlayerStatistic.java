package ggp.database.statistics.statistic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.appengine.api.datastore.Entity;
import external.JSON.JSONException;
import external.JSON.JSONObject;

public abstract class PerPlayerStatistic<T extends Statistic> extends Statistic {
    private static final String playerPrefix = "player_";
    
    Map<String, T> cachedPlayerStats = new HashMap<String, T>();
    public T getPerPlayerStatistic(String playerName) {
        try {
            if (cachedPlayerStats.containsKey(playerName)) {
                return cachedPlayerStats.get(playerName);
            } else {                
                T aStat = getInitialStatistic();
                if (getState().has(playerPrefix + playerName)) {
                    aStat.setState(getState().getJSONObject(playerPrefix + playerName));
                } else {
                    getState().put(playerPrefix + playerName, aStat.getState());
                }
                cachedPlayerStats.put(playerName, aStat);
                return aStat;
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    protected Object getFinalForm() throws JSONException {
        JSONObject theFinalForm = new JSONObject();
        for (String playerName : getKnownPlayerNames()) {
            theFinalForm.put(playerName, getPerPlayerStatistic(playerName).getFinalForm());
        }
        return theFinalForm;
    }
    
    @Override
    protected final Object getPerPlayerFinalForm(String forPlayer) throws JSONException {
        return getPerPlayerStatistic(forPlayer).getFinalForm();
    }

    public Set<String> getKnownPlayerNames() {
        Set<String> theKnownPlayers = new HashSet<String>();
        Iterator<?> i = getState().keys();
        while(i.hasNext()) {
            String theKey = i.next().toString();
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
        	if (!thePlayerNames.get(i).isEmpty()) {
        		playerNames.add(thePlayerNames.get(i));
        	}
        }
        return playerNames;
    }
    
    public abstract void updateWithMatch(Entity newMatch);
    protected abstract T getInitialStatistic();    
}