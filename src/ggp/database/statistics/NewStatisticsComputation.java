package ggp.database.statistics;

import ggp.database.statistics.statistic.Statistic;
import ggp.database.statistics.stored.FinalGameStats;
import ggp.database.statistics.stored.FinalOverallStats;
import ggp.database.statistics.stored.FinalPlayerStats;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Text;

import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

public class NewStatisticsComputation {
    public static final int STATS_VERSION = 1;

    public static String getHostName(String matchHostPK) {
        if (matchHostPK.contains("0ca7065b86d7646166d86233f9e23ac47d8320d4")) return "SimpleGameSim";
        if (matchHostPK.contains("90bd08a7df7b8113a45f1e537c1853c3974006b2")) return "Apollo";
        return matchHostPK;
    }

    public static void computeBatchStatistics() {        
        DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();        
        Query q = new Query("CondensedMatch");
        q.addSort("startTime");
        PreparedQuery pq = datastore.prepare(q);

        // Generate a fresh set of statistics computations.
        Map<String, NewStatisticsComputation> statsForLabel = new HashMap<String, NewStatisticsComputation>();
        statsForLabel.put("all", new NewStatisticsComputation());
        statsForLabel.put("unsigned", new NewStatisticsComputation());

        // Compute statistics by iterating over all of the stored matches in order.
        for (Entity result : pq.asIterable()) {
            JSONObject theJSON = null;
            try {
                theJSON = new JSONObject(((Text)result.getProperty("theMatchJSON")).getValue());
            } catch (Exception e) {
                continue;
            }
            
            addAdditionalMatch(statsForLabel, theJSON);
        }
        
        // Save all of the computations for future use.
        for (String theLabel : statsForLabel.keySet()) {
            statsForLabel.get(theLabel).saveAs(theLabel);
        }
    }
    
    public static void incrementallyAddMatch(JSONObject theMatchJSON) {
        // Load the stored set of statistics computations.
        Map<String, NewStatisticsComputation> statsForLabel = new HashMap<String, NewStatisticsComputation>();
        Set<IntermediateStatistics> theIntermediates = IntermediateStatistics.loadAllIntermediateStatistics();
        for (IntermediateStatistics s : theIntermediates) {
            NewStatisticsComputation r = new NewStatisticsComputation();
            r.restoreFrom(s.getIntermediateData());
            statsForLabel.put(s.getKey(), r);
        }
        
        // Compute statistics by iterating over all of the stored matches in order.
        addAdditionalMatch(statsForLabel, theMatchJSON);

        // Save all of the computations for future use.
        for (String theLabel : statsForLabel.keySet()) {
            statsForLabel.get(theLabel).saveAs(theLabel);
        }        
    }
    
    private static void addAdditionalMatch(Map<String, NewStatisticsComputation> statsForLabel, JSONObject theJSON) {
        statsForLabel.get("all").add(theJSON, true);
        try {
            if (theJSON.has("matchHostPK")) {
              String theHostPK = theJSON.getString("matchHostPK");
              if (!statsForLabel.containsKey(theHostPK)) {
                statsForLabel.put(theHostPK, new NewStatisticsComputation());
              }
              statsForLabel.get(theHostPK).add(theJSON, false);
            } else {
              statsForLabel.get("unsigned").add(theJSON, true);
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }   
    }

    private Set<String> theGames;
    private Set<String> thePlayers;
    private Set<Statistic> registeredStatistics;
    
    public NewStatisticsComputation () {
        registeredStatistics = new HashSet<Statistic>();
        // TODO: add things to registeredStatistics
    }

    public void add(JSONObject theJSON, boolean addHostToPlayerNames) {
        try {            
            for (Statistic s : registeredStatistics) {
                s.updateWithMatch(theJSON);
            }
        } catch(JSONException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public void restoreFrom(JSONObject serializedState) {
        for (Statistic s : registeredStatistics) {
            s.loadState(serializedState);
        }
        
        // TODO: reconstruct theGames, somehow
        // TODO: reconstruct thePlayers, somehow
    }
    
    public void saveAs(String theLabel) {
        // Store the statistics as a JSON object in the datastore.
        try {
            // Store the intermediate statistics
            JSONObject serializedState = new JSONObject();
            for (Statistic s : registeredStatistics) {
                s.saveState(serializedState);
            }
            IntermediateStatistics.saveIntermediateStatistics(theLabel, serializedState);
            
            // Store the overall statistics
            JSONObject overallStats = new JSONObject();
            for (Statistic s : registeredStatistics) {
                overallStats.put(s.getClass().getSimpleName(), s.getFinalForm());                
            }
            FinalOverallStats.load(theLabel).setJSON(overallStats);

            // Store the per-game statistics
            for (String gameName : theGames) {
                JSONObject gameStats = new JSONObject();
                for (Statistic s : registeredStatistics) {
                    gameStats.put(s.getClass().getSimpleName(), s.getPerPlayerFinalForm(gameName));
                }
                FinalGameStats.load(theLabel, gameName).setJSON(gameStats);
            }

            // Store the per-player statistics
            for (String playerName : thePlayers) {
                JSONObject playerStats = new JSONObject();
                for (Statistic s : registeredStatistics) {
                    playerStats.put(s.getClass().getSimpleName(), s.getPerPlayerFinalForm(playerName));
                }
                FinalPlayerStats.load(theLabel, playerName).setJSON(playerStats);
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }        
    }
}