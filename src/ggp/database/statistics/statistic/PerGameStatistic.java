package ggp.database.statistics.statistic;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.appengine.api.datastore.Entity;
import org.json.JSONException;
import org.json.JSONObject;

public abstract class PerGameStatistic<T extends Statistic> extends Statistic {
    private static final String gamePrefix = "game_";
    
    Map<String, T> cachedGameStats = new HashMap<String, T>();
    public T getPerGameStatistic(String gameName) {
        try {
            if (cachedGameStats.containsKey(gameName)) {
                return cachedGameStats.get(gameName);
            } else {
                T aStat = getInitialStatistic();
                if (getState().has(gamePrefix + gameName)) {
                    aStat.setState(getState().getJSONObject(gamePrefix + gameName));
                } else {
                    getState().put(gamePrefix + gameName, aStat.getState());
                }
                cachedGameStats.put(gameName, aStat);
                return aStat;
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    protected Object getFinalForm() throws JSONException {
        JSONObject theFinalForm = new JSONObject();
        for (String gameName : getKnownGameNames()) {
            theFinalForm.put(gameName, getPerGameStatistic(gameName).getFinalForm());
        }
        return theFinalForm;
    }
    
    @Override
    protected final Object getPerGameFinalForm(String forGame) throws JSONException {
        return getPerGameStatistic(forGame).getFinalForm();
    }

    @SuppressWarnings("unchecked")
    public Set<String> getKnownGameNames() {
        Set<String> theKnownGames = new HashSet<String>();
        Iterator<String> i = getState().keys();
        while(i.hasNext()) {
            String theKey = i.next();
            if (theKey.startsWith(gamePrefix)) {
                theKnownGames.add(theKey.replaceFirst(gamePrefix, ""));
            }
        }
        return theKnownGames;
    }

    public abstract void updateWithMatch(Entity newMatch);
    protected abstract T getInitialStatistic();    
}