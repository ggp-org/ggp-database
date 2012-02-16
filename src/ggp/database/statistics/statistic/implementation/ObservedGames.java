package ggp.database.statistics.statistic.implementation;

import java.util.HashSet;
import java.util.Set;

import ggp.database.statistics.statistic.Statistic;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.repackaged.org.json.JSONArray;
import com.google.appengine.repackaged.org.json.JSONException;

public class ObservedGames extends Statistic {
    private Set<String> theGames = new HashSet<String>();
    
    public ObservedGames() {
        try {
            if (!getState().has("theGames")) {
                getState().put("theGames", new JSONArray());
            }
            JSONArray theGamesArray = getState().getJSONArray("theGames");
            for (int i = 0; i < theGamesArray.length(); i++) {
                theGames.add(theGamesArray.getString(i));
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
    
    public Set<String> getGames() {
        return theGames;
    }
    
    @Override public void updateWithMatch(Entity newMatch) {
        String theGame = newMatch.getProperty("gameMetaURL").toString();
        if (!theGames.contains(theGame)) {
            theGames.add(theGame);
            try {
                getState().getJSONArray("theGames").put(theGame);
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    @Override public Object getFinalForm() throws JSONException {
        return getState().getJSONArray("theGames");
    }
}