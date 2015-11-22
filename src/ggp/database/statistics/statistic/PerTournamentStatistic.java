package ggp.database.statistics.statistic;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.appengine.api.datastore.Entity;
import external.JSON.JSONException;
import external.JSON.JSONObject;

public abstract class PerTournamentStatistic<T extends Statistic> extends Statistic {
    private static final String tournamentPrefix = "tournament_";
    
    Map<String, T> cachedTournamentStats = new HashMap<String, T>();
    public T getPerTournamentStatistic(String tournamentName) {
        try {
            if (cachedTournamentStats.containsKey(tournamentName)) {
                return cachedTournamentStats.get(tournamentName);
            } else {
                T aStat = getInitialStatistic();
                if (getState().has(tournamentPrefix + tournamentName)) {
                    aStat.setState(getState().getJSONObject(tournamentPrefix + tournamentName));
                } else {
                    getState().put(tournamentPrefix + tournamentName, aStat.getState());
                }
                cachedTournamentStats.put(tournamentName, aStat);
                return aStat;
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    protected Object getFinalForm() throws JSONException {
        JSONObject theFinalForm = new JSONObject();
        for (String tournamentName : getKnownTournamentNames()) {
            theFinalForm.put(tournamentName, getPerTournamentStatistic(tournamentName).getFinalForm());
        }
        return theFinalForm;
    }
    
    @Override
    protected final Object getPerTournamentFinalForm(String forTournament) throws JSONException {
        return getPerTournamentStatistic(forTournament).getFinalForm();
    }

    public Set<String> getKnownTournamentNames() {
        Set<String> theKnownGames = new HashSet<String>();
        Iterator<?> i = getState().keys();
        while(i.hasNext()) {
            String theKey = i.next().toString();
            if (theKey.startsWith(tournamentPrefix)) {
                theKnownGames.add(theKey.replaceFirst(tournamentPrefix, ""));
            }
        }
        return theKnownGames;
    }

    public abstract void updateWithMatch(Entity newMatch);
    protected abstract T getInitialStatistic();    
}