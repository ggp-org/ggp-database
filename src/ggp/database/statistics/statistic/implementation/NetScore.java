package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.PerPlayerStatistic;

import java.util.List;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.repackaged.org.json.JSONException;

public class NetScore extends PerPlayerStatistic {
    @SuppressWarnings("unchecked")
    public void updateWithMatch(Entity newMatch) {
        if (newMatch.getProperty("goalValues") == null) return;        
        if (!(Boolean)newMatch.getProperty("isCompleted")) return;
        if (newMatch.getProperty("hashedMatchHostPK") == null) return;
        if ((Boolean)newMatch.getProperty("hasErrors")) return;

        List<String> playerNames = getPlayerNames(newMatch);
        if (playerNames == null) return;
        
        for (int i = 0; i < playerNames.size(); i++) {
            double theScore = ((List<Long>)newMatch.getProperty("goalValues")).get(i);
            incrementPerPlayerVariable(playerNames.get(i), "netScore", (theScore-50.0)/50.0);
        }
    }

    @Override
    public Object getPerPlayerFinalForm(String forPlayer) {
        try {
            return getPerPlayerState(forPlayer).getDouble("netScore");
        } catch (JSONException e) {
            return null;
        }
    }
}