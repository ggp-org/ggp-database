package ggp.database.statistics.statistic.implementation;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import ggp.database.statistics.statistic.PerPlayerStatistic;
import ggp.database.statistics.statistic.Statistic;

import com.google.appengine.api.datastore.Entity;
import org.json.JSONArray;
import org.json.JSONException;

public class ObservedPlayers extends Statistic {
    private Set<String> thePlayers;
    
    public void makeInitialized() {
        if (thePlayers != null)
            return;
        try {
            thePlayers = new HashSet<String>();
            if (!getState().has("thePlayers")) {
                getState().put("thePlayers", new JSONArray());
            }
            JSONArray thePlayersArray = getState().getJSONArray("thePlayers");
            for (int i = 0; i < thePlayersArray.length(); i++) {
                thePlayers.add(thePlayersArray.getString(i));
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
    
    public Set<String> getPlayers() {
        makeInitialized();
        return thePlayers;
    }    
    
    @Override public void updateWithMatch(Entity newMatch) {
        makeInitialized();
        List<String> theMatchPlayers = PerPlayerStatistic.getPlayerNames(newMatch);
        for (String playerName : theMatchPlayers) {
            if (!thePlayers.contains(playerName)) {
                thePlayers.add(playerName);
                try {
                    getState().getJSONArray("thePlayers").put(playerName);
                } catch (JSONException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
    
    @Override public Object getFinalForm() throws JSONException {
        return getState().getJSONArray("thePlayers");
    }    
}