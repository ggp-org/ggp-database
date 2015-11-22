package ggp.database.statistics.statistic.implementation;

import java.util.HashSet;
import java.util.Set;

import ggp.database.statistics.statistic.Statistic;

import com.google.appengine.api.datastore.Entity;
import external.JSON.JSONException;
import external.JSON.JSONArray;

public class ObservedTournaments extends Statistic {
    private Set<String> theTournaments;
    
    private void makeInitialized() {
        if (theTournaments != null)
            return;
        try {
        	theTournaments = new HashSet<String>();
            if (!getState().has("theTournaments")) {
                getState().put("theTournaments", new JSONArray());
            }
            JSONArray theGamesArray = getState().getJSONArray("theTournaments");
            for (int i = 0; i < theGamesArray.length(); i++) {
            	theTournaments.add(theGamesArray.getString(i));
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
    
    public Set<String> theTournaments() {
        makeInitialized();
        return theTournaments;
    }
    
    @Override public void updateWithMatch(Entity newMatch) {
        makeInitialized();
        if (newMatch.getProperty("tournamentNameFromHost") == null) return;
        String theTournament = newMatch.getProperty("tournamentNameFromHost").toString();
        if (!theTournaments.contains(theTournament)) {
        	theTournaments.add(theTournament);
            try {
                getState().getJSONArray("theTournaments").put(theTournament);
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    @Override public Object getFinalForm() throws JSONException {
        return getState().getJSONArray("theTournaments");
    }
}