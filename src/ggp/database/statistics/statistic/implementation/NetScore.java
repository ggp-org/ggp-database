package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.PerEntityStatistic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

public class NetScore extends PerEntityStatistic {
    public void updateWithMatch(JSONObject newMatch) throws JSONException {
        List<String> playerNames = getPlayerNames(newMatch);
        if (playerNames == null) return;
        
        for (int i = 0; i < playerNames.size(); i++) {
            incrementPerPlayerVariable(playerNames.get(i), "netScore", newMatch.getJSONArray("goalValues").getInt(i));
        }
    }

    @Override
    public Object getFinalForm() throws JSONException {
        // TODO: refactor this so it's done at the PerPlayerStatistic level
        Map<String,Integer> netScores = new HashMap<String,Integer>();
        for (String playerName : getKnownPlayerNames()) {
            netScores.put(playerName, getPerPlayerState(playerName).getInt("netScore"));
        }
        return netScores;
    }

    @Override
    public Object getPerGameFinalForm(String forGame) throws JSONException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object getPerPlayerFinalForm(String forPlayer) throws JSONException {
        return getPerPlayerState(forPlayer).getInt("netScore");
    }
}