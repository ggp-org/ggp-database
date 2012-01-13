package ggp.database.statistics;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withChunkSize;
import ggp.database.statistics.statistic.Statistic;
import ggp.database.statistics.statistic.implementation.ComputeRAM;
import ggp.database.statistics.statistic.implementation.ComputeTime;
import ggp.database.statistics.statistic.implementation.ComputedAt;
import ggp.database.statistics.statistic.implementation.Matches;
import ggp.database.statistics.statistic.implementation.MatchesAbandoned;
import ggp.database.statistics.statistic.implementation.MatchesAverageMoves;
import ggp.database.statistics.statistic.implementation.MatchesAveragePlayers;
import ggp.database.statistics.statistic.implementation.MatchesFinished;
import ggp.database.statistics.statistic.implementation.MatchesStatErrors;
import ggp.database.statistics.statistic.implementation.NetScore;
import ggp.database.statistics.statistic.implementation.ObservedGames;
import ggp.database.statistics.statistic.implementation.ObservedPlayers;
import ggp.database.statistics.statistic.implementation.StatsVersion;
import ggp.database.statistics.stored.FinalGameStats;
import ggp.database.statistics.stored.FinalOverallStats;
import ggp.database.statistics.stored.FinalPlayerStats;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.api.datastore.ReadPolicy;

import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

/*
 * Still need to convert:
 *   leaderboard
 *   decayedLeaderboard
 *   matchesInPastHour
 *   matchesInPastDay
 *   matchesPerDayMedian
 *   eloRank
 *   agonSkill
 *   agonScaledSkill
 *   agonDifficulty
 *   agonScaledDifficulty
 *   matchesStartedChart
*/

public class NewStatisticsComputation implements Statistic.Reader {
    public static void computeBatchStatistics() throws IOException {
        Map<String, NewStatisticsComputation> statsForLabel = new HashMap<String, NewStatisticsComputation>();
        statsForLabel.put("all", new NewStatisticsComputation());
        statsForLabel.put("unsigned", new NewStatisticsComputation());
        
        long nComputeBeganAt = System.currentTimeMillis();
        {
            DatastoreService datastore = DatastoreServiceFactory.getDatastoreService(DatastoreServiceConfig.Builder.withReadPolicy(new ReadPolicy(ReadPolicy.Consistency.EVENTUAL)));        
            Query q = new Query("CondensedMatch");
            q.addSort("startTime");
            PreparedQuery pq = datastore.prepare(q);
            
            long startTime;
            String cursorForContinuation = "";
            
            do {            
                QueryResultIterator<Entity> results;
                if (cursorForContinuation.isEmpty()) {
                    results = pq.asQueryResultIterator(withChunkSize(1000));
                } else {
                    results = pq.asQueryResultIterator(withChunkSize(1000).startCursor(Cursor.fromWebSafeString(cursorForContinuation)));
                }
            
                // Continuation data, so we can pull in multiple queries worth of data
                startTime = System.currentTimeMillis();
                cursorForContinuation = "";
                
                /* Compute the statistics in a single sorted pass over the data */            
                while (results.hasNext()) {
                    if (System.currentTimeMillis() - startTime > 20000) {
                        cursorForContinuation = results.getCursor().toWebSafeString();
                        break;
                    }
                    
                    addAdditionalMatch(statsForLabel, results.next());                    
                }            
            } while (!cursorForContinuation.isEmpty());
        }
        long nComputeFinishedAt = System.currentTimeMillis();
        
        // Save all of the statistics
        for (String theLabel : statsForLabel.keySet()) {
            statsForLabel.get(theLabel).getStatistic(ComputeTime.class).incrementComputeTime(nComputeFinishedAt - nComputeBeganAt);
            statsForLabel.get(theLabel).finalizeComputation();
            statsForLabel.get(theLabel).saveAs(theLabel);
        }        
    }
    
    /*
    public static void incrementallyAddMatch(Entity newMatch) {
        // Load the stored set of statistics computations.
        Map<String, NewStatisticsComputation> statsForLabel = new HashMap<String, NewStatisticsComputation>();
        Set<IntermediateStatistics> theIntermediates = IntermediateStatistics.loadAllIntermediateStatistics();
        for (IntermediateStatistics s : theIntermediates) {
            NewStatisticsComputation r = new NewStatisticsComputation();
            r.restoreFrom(s.getIntermediateData());
            statsForLabel.put(s.getKey(), r);
        }
        
        // Compute statistics by iterating over all of the stored matches in order.
        long nComputeBeganAt = System.currentTimeMillis();
        addAdditionalMatch(statsForLabel, newMatch);
        long nComputeFinishedAt = System.currentTimeMillis();

        // Save all of the computations for future use.
        for (String theLabel : statsForLabel.keySet()) {
            statsForLabel.get(theLabel).getStatistic(ComputationTime.class).incrementComputeTime(nComputeFinishedAt - nComputeBeganAt);
            statsForLabel.get(theLabel).finalizeComputation();
            statsForLabel.get(theLabel).saveAs(theLabel);
        }        
    }
    */

    private static void addAdditionalMatch(Map<String, NewStatisticsComputation> statsForLabel, Entity match) {
        statsForLabel.get("all").add(match);
        if (match.getProperty("hashedMatchHostPK") != null) {
          String theHostPK = (String)match.getProperty("hashedMatchHostPK");
          if (!statsForLabel.containsKey(theHostPK)) {
            statsForLabel.put(theHostPK, new NewStatisticsComputation());
          }
          statsForLabel.get(theHostPK).add(match);
        } else {
          statsForLabel.get("unsigned").add(match);
        }        
    }

    private Set<Statistic> registeredStatistics;
    
    public NewStatisticsComputation () {
        registeredStatistics = new HashSet<Statistic>();
        /* TODO: get this working
        for (Statistic aStat : ServiceLoader.load(Statistic.class)) {
            registeredStatistics.add(aStat);
        }*/
        registeredStatistics.add(new ComputedAt());
        registeredStatistics.add(new ComputeRAM());
        registeredStatistics.add(new ComputeTime());
        registeredStatistics.add(new Matches());
        registeredStatistics.add(new MatchesAbandoned());
        registeredStatistics.add(new MatchesAverageMoves());
        registeredStatistics.add(new MatchesAveragePlayers());
        registeredStatistics.add(new MatchesFinished());
        registeredStatistics.add(new MatchesStatErrors());
        registeredStatistics.add(new NetScore());
        registeredStatistics.add(new ObservedGames());
        registeredStatistics.add(new ObservedPlayers());
        registeredStatistics.add(new StatsVersion());
    }
    
    @SuppressWarnings("unchecked")
    public <T extends Statistic> T getStatistic(Class<T> c) {
        for (Statistic s : registeredStatistics) {
            if (s.getClass().equals(c))
                return (T)s;
        }
        return null;
    }

    public void add(Entity theMatch) {
        for (Statistic s : registeredStatistics) {
            s.updateWithMatch(theMatch);
        }
    }
    
    public void finalizeComputation() {
        for (Statistic s : registeredStatistics) {
            s.finalizeComputation(this);
        }
    }
    
    /*
    public void restoreFrom(JSONObject serializedState) {
        for (Statistic s : registeredStatistics) {
            s.loadState(serializedState);
        }
    }
    */
    
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
                overallStats.put(camelCase(s.getClass().getSimpleName()), s.getFinalForm());                
            }
            FinalOverallStats.load("x_" + theLabel).setJSON(overallStats);

            // Store the per-game statistics
            for (String gameName : getStatistic(ObservedGames.class).getGames()) {
                JSONObject gameStats = new JSONObject();
                for (Statistic s : registeredStatistics) {
                    gameStats.put(camelCase(s.getClass().getSimpleName()), s.getPerPlayerFinalForm(gameName));
                }
                FinalGameStats.load("x_" + theLabel, gameName).setJSON(gameStats);
            }

            // Store the per-player statistics
            for (String playerName : getStatistic(ObservedPlayers.class).getPlayers()) {
                JSONObject playerStats = new JSONObject();
                for (Statistic s : registeredStatistics) {
                    playerStats.put(camelCase(s.getClass().getSimpleName()), s.getPerPlayerFinalForm(playerName));
                }
                FinalPlayerStats.load("x_" + theLabel, playerName).setJSON(playerStats);
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }        
    }
    
    public static String camelCase(String x) {
        return x.substring(0,1).toLowerCase() + x.substring(1);
    }
}