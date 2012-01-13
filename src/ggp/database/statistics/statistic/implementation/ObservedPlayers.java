package ggp.database.statistics.statistic.implementation;

import java.util.HashSet;
import java.util.Set;

import ggp.database.statistics.statistic.PerPlayerStatistic;
import ggp.database.statistics.statistic.Statistic;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.repackaged.org.json.JSONArray;
import com.google.appengine.repackaged.org.json.JSONException;

public class ObservedPlayers extends Statistic {
    private Set<String> thePlayers = new HashSet<String>();
    
    public ObservedPlayers() {
        try {
            JSONArray theGamesArray = getState().getJSONArray("thePlayers");
            for (int i = 0; i < theGamesArray.length(); i++) {
                thePlayers.add(theGamesArray.getString(i));
            }
        } catch (JSONException e) {
            ;
        }
    }
    
    public Set<String> getPlayers() {
        return thePlayers;
    }    
    
    @Override public void updateWithMatch(Entity newMatch) {
        thePlayers.addAll(PerPlayerStatistic.getPlayerNames(newMatch));
    }
    
    @Override public void finalizeComputation(Statistic.Reader theReader) {
        try {
            getState().put("thePlayers", thePlayers);
        } catch (JSONException e) {
            ;
        }
    }
    
    @Override public Object getFinalForm() throws JSONException {
        return getState().getJSONArray("thePlayers");
    }    
}