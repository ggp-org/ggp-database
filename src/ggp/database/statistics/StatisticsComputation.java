package ggp.database.statistics;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withChunkSize;
import ggp.database.statistics.stored.FinalGameStats;
import ggp.database.statistics.stored.FinalOverallStats;
import ggp.database.statistics.stored.FinalPlayerStats;
import ggp.database.statistics.counters.AgonRank;
import ggp.database.statistics.counters.EloRank;
import ggp.database.statistics.counters.MedianPerDay;
import ggp.database.statistics.counters.TimeBucketSeries;
import ggp.database.statistics.counters.WeightedAverage;
import ggp.database.statistics.counters.WinLossCounter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
import com.google.appengine.repackaged.com.google.common.base.Pair;
import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

public class StatisticsComputation {
    public static final int STATS_VERSION = 15;

    public static String getHostName(String matchHostPK) {
        if (matchHostPK.contains("0ca7065b86d7646166d86233f9e23ac47d8320d4")) return "Sample";
        if (matchHostPK.contains("90bd08a7df7b8113a45f1e537c1853c3974006b2")) return "Apollo";
        if (matchHostPK.contains("f69721b2f73839e513eed991e96824f1af218ac1")) return "Dresden";
        if (matchHostPK.contains("5bc94f8e793772e8585a444f2fc95d2ac087fed0")) return "Artemis";
        return matchHostPK;
    }

    public static void computeStatistics() throws IOException {
        Map<String, StatisticsComputation> statsForLabel = new HashMap<String, StatisticsComputation>();
        statsForLabel.put("all", new StatisticsComputation());
        statsForLabel.put("unsigned", new StatisticsComputation());
        
        nComputeBeganAt = System.currentTimeMillis();
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
                    
                    Entity result = results.next();
                    statsForLabel.get("all").add(result, true);
                    if (result.getProperty("hashedMatchHostPK") != null) {
                      String theHostPK = (String)result.getProperty("hashedMatchHostPK");
                      if (!statsForLabel.containsKey(theHostPK)) {
                        statsForLabel.put(theHostPK, new StatisticsComputation());
                      }
                      statsForLabel.get(theHostPK).add(result, false);
                    } else {
                      statsForLabel.get("unsigned").add(result, true);
                    }
                }            
            } while (!cursorForContinuation.isEmpty());
        }
        nComputeFinishedAt = System.currentTimeMillis();
        
        // Save all of the statistics        
        for (String theLabel : statsForLabel.keySet()) {
            statsForLabel.get(theLabel).finalStageComputation();
            statsForLabel.get(theLabel).saveAs(theLabel);
        }        
    }
    
    /* non static methods */
    int nMatches = 0;
    int nMatchesFinished = 0;
    int nMatchesAbandoned = 0;
    int nMatchesStatErrors = 0;
    int nMatchesInPastHour = 0;
    int nMatchesInPastDay = 0;        

    MedianPerDay matchesPerDay = new MedianPerDay();
    Map<String,WeightedAverage> playerAverageScore = new HashMap<String,WeightedAverage>();
    Map<String,WeightedAverage> playerDecayedAverageScore = new HashMap<String,WeightedAverage>();
    Map<String,Map<String,WeightedAverage>> computedEdgeOn = new HashMap<String,Map<String,WeightedAverage>>();
    Map<String,Map<String,WeightedAverage>> averageScoreOn = new HashMap<String,Map<String,WeightedAverage>>();    
    Map<String,Map<String,WeightedAverage>> averageScoreVersus = new HashMap<String,Map<String,WeightedAverage>>();    
    Map<String,WeightedAverage> gameAverageMoves = new HashMap<String,WeightedAverage>();
    Map<String,Map<String,Map<String,WinLossCounter>>> playerWinsVersusPlayerOnGame = new HashMap<String,Map<String,Map<String,WinLossCounter>>>();
    Map<String,Double> netScores = new HashMap<String,Double>();
    Map<String,Map<Integer,Map<String,WeightedAverage>>> perGameRolePlayerAverageScore = new HashMap<String,Map<Integer,Map<String,WeightedAverage>>>();
    Map<String,List<Double>> perGameRoleCorrelationWithSkill = new HashMap<String,List<Double>>();

    Set<String> theGameNames = new HashSet<String>();
    Set<String> thePlayerNames = new HashSet<String>();
    WeightedAverage playersPerMatch = new WeightedAverage();
    WeightedAverage movesPerMatch = new WeightedAverage();
    EloRank theEloRank = new EloRank();
    AgonRank theAgonRank = new AgonRank();
    
    TimeBucketSeries matchesStarted = new TimeBucketSeries();

    static long nComputeBeganAt, nComputeFinishedAt;

    @SuppressWarnings("unchecked")
    public void add(Entity theMatch, boolean addHostToPlayerNames) {
        nMatches++;
        matchesPerDay.addToDay(1, (Long)theMatch.getProperty("startTime"));
        if (System.currentTimeMillis() - (Long)theMatch.getProperty("startTime") < 3600000L) nMatchesInPastHour++;
        if (System.currentTimeMillis() - (Long)theMatch.getProperty("startTime") < 86400000L) nMatchesInPastDay++;
        
        // TODO: Use a better way to determine the number of players in the match,
        // that works for matches that haven't been signed.
        if (theMatch.getProperty("playerNamesFromHost") != null) {
            playersPerMatch.addValue(((List<String>)theMatch.getProperty("playerNamesFromHost")).size());
        }
        
        if ((Boolean)theMatch.getProperty("isCompleted")) {
            nMatchesFinished++;
            movesPerMatch.addValue((Long)theMatch.getProperty("moveCount"));

            String theGame = (String)theMatch.getProperty("gameMetaURL");
            if (!gameAverageMoves.containsKey(theGame)) {
                gameAverageMoves.put(theGame, new WeightedAverage());
            }
            gameAverageMoves.get(theGame).addValue((Long)theMatch.getProperty("moveCount"));
            theGameNames.add(theGame);

            boolean matchHadErrors = (Boolean)theMatch.getProperty("hasErrors");                
            matchesStarted.addEntry((Long)theMatch.getProperty("startTime"));

            // Only compute score-related statistics for matches with named players
            // within a specified player namespace.
            if (theMatch.getProperty("playerNamesFromHost") != null && theMatch.getProperty("hashedMatchHostPK") != null) {
                List<String> matchPlayers = new ArrayList<String>();
                for (int i = 0; i < ((List<String>)theMatch.getProperty("playerNamesFromHost")).size(); i++) {
                    matchPlayers.add( (addHostToPlayerNames ? getHostName((String)theMatch.getProperty("hashedMatchHostPK"))+"." : "") + ((List<String>)theMatch.getProperty("playerNamesFromHost")).get(i));
                }
                thePlayerNames.addAll(matchPlayers);

                // Score-related statistics.                        
                for (int i = 0; i < matchPlayers.size(); i++) {
                    String aPlayer = matchPlayers.get(i);
                    int aPlayerScore = ((List<Long>)theMatch.getProperty("goalValues")).get(i).intValue();

                    if (!matchHadErrors) {
                        if (!netScores.containsKey(aPlayer)) {
                            netScores.put(aPlayer, 0.0);
                        }
                        netScores.put(aPlayer, netScores.get(aPlayer) + (aPlayerScore-50.0)/50.0);
                    }

                    if (!playerAverageScore.containsKey(aPlayer)) {
                        playerAverageScore.put(aPlayer, new WeightedAverage());
                    }
                    playerAverageScore.get(aPlayer).addValue(aPlayerScore);

                    if (!perGameRolePlayerAverageScore.containsKey(theGame)) {
                        perGameRolePlayerAverageScore.put(theGame, new HashMap<Integer,Map<String,WeightedAverage>>());
                    }
                    if (!perGameRolePlayerAverageScore.get(theGame).containsKey(i)) {
                        perGameRolePlayerAverageScore.get(theGame).put(i, new HashMap<String,WeightedAverage>());
                    }
                    if (!perGameRolePlayerAverageScore.get(theGame).get(i).containsKey(aPlayer)) {
                        perGameRolePlayerAverageScore.get(theGame).get(i).put(aPlayer, new WeightedAverage());
                    }
                    perGameRolePlayerAverageScore.get(theGame).get(i).get(aPlayer).addValue(aPlayerScore);
                    
                    double ageInDays = (double)(System.currentTimeMillis() - (Long)theMatch.getProperty("startTime")) / (double)(86400000L);
                    if (!playerDecayedAverageScore.containsKey(aPlayer)) {
                        playerDecayedAverageScore.put(aPlayer, new WeightedAverage());
                    }
                    playerDecayedAverageScore.get(aPlayer).addValue(aPlayerScore, Math.pow(0.98, ageInDays));

                    if (!averageScoreOn.containsKey(aPlayer)) {
                        averageScoreOn.put(aPlayer, new HashMap<String,WeightedAverage>());
                    }
                    if (!averageScoreOn.get(aPlayer).containsKey(theGame)) {
                        averageScoreOn.get(aPlayer).put(theGame, new WeightedAverage());
                    }
                    averageScoreOn.get(aPlayer).get(theGame).addValue(aPlayerScore);

                    for (int j = 0; j < matchPlayers.size(); j++) {
                        String bPlayer = matchPlayers.get(j);
                        int bPlayerScore = ((List<Long>)theMatch.getProperty("goalValues")).get(j).intValue();
                        if (bPlayer.equals(aPlayer))
                            continue;

                        if (!averageScoreVersus.containsKey(aPlayer)) {
                            averageScoreVersus.put(aPlayer, new HashMap<String,WeightedAverage>());
                        }
                        if (!averageScoreVersus.get(aPlayer).containsKey(bPlayer)) {
                            averageScoreVersus.get(aPlayer).put(bPlayer, new WeightedAverage());
                        }
                        averageScoreVersus.get(aPlayer).get(bPlayer).addValue(aPlayerScore);

                        if (!playerWinsVersusPlayerOnGame.containsKey(aPlayer)) {
                            playerWinsVersusPlayerOnGame.put(aPlayer, new HashMap<String,Map<String,WinLossCounter>>());
                        }
                        if (!playerWinsVersusPlayerOnGame.get(aPlayer).containsKey(bPlayer)) {
                            playerWinsVersusPlayerOnGame.get(aPlayer).put(bPlayer, new HashMap<String,WinLossCounter>());
                        }
                        if (!playerWinsVersusPlayerOnGame.get(aPlayer).get(bPlayer).containsKey(theGame)) {
                            playerWinsVersusPlayerOnGame.get(aPlayer).get(bPlayer).put(theGame, new WinLossCounter());
                        }
                        playerWinsVersusPlayerOnGame.get(aPlayer).get(bPlayer).get(theGame).addEntry(aPlayerScore, bPlayerScore);
                        
                        // TODO: Really this should be "zero-sum game"?
                        if (aPlayerScore + bPlayerScore == 100) {
                            theEloRank.addNextMatch(aPlayer, bPlayer, aPlayerScore, bPlayerScore);
                            theAgonRank.addNextMatch(aPlayer, bPlayer, aPlayerScore, bPlayerScore);
                        
                            // Update the computed edge for the player on the game.
                            if (!computedEdgeOn.containsKey(aPlayer)) {
                                computedEdgeOn.put(aPlayer, new HashMap<String,WeightedAverage>());
                            }
                            if (!computedEdgeOn.get(aPlayer).containsKey(theGame)) {
                                computedEdgeOn.get(aPlayer).put(theGame, new WeightedAverage());
                            }
                            double matchEdgeWeight = 1.0/(1.0+Math.exp(theAgonRank.getComputedSkill(aPlayer)-theAgonRank.getComputedSkill(bPlayer)));
                            computedEdgeOn.get(aPlayer).get(theGame).addValue(aPlayerScore-bPlayerScore, matchEdgeWeight);
                        }
                    }
                    
                    theAgonRank.addMatchParticipation(aPlayer, theGame, i, aPlayerScore);
                }
            }
        } else {
            nMatchesAbandoned++;
        }        
    }
    
    public void finalStageComputation() {
        // Perform any computations that need to be done after adding all the games.
        for (String aGame : perGameRolePlayerAverageScore.keySet()) {
            perGameRoleCorrelationWithSkill.put(aGame, new ArrayList<Double>());            
            for (int i = 0; i < perGameRolePlayerAverageScore.get(aGame).size(); i++) {
                Map<String,WeightedAverage> thePlayerAverageScores = perGameRolePlayerAverageScore.get(aGame).get(i);
                Set<Pair<Double,Double>> dataPoints = new HashSet<Pair<Double,Double>>();
                WeightedAverage meanScore = new WeightedAverage();
                WeightedAverage meanSkill = new WeightedAverage();
                for (String aPlayer : thePlayerAverageScores.keySet()) {
                    double theScore = thePlayerAverageScores.get(aPlayer).getWeightedAverage();
                    double theSkill = theAgonRank.getSkills().get(aPlayer);
                    dataPoints.add(new Pair<Double,Double>(theScore, theSkill));
                    meanScore.addValue(theScore);
                    meanSkill.addValue(theSkill);                    
                }
                double xBar = meanScore.getWeightedAverage();
                double yBar = meanSkill.getWeightedAverage();
                double A=0, B=0, C=0;
                for (Pair<Double,Double> aPoint : dataPoints) {
                    A += (aPoint.first - xBar)*(aPoint.second - yBar);
                    B += (aPoint.first - xBar)*(aPoint.first - xBar);
                    C += (aPoint.second - yBar)*(aPoint.second - yBar);
                }
                double theCorrelation = A/Math.sqrt(B*C);
                perGameRoleCorrelationWithSkill.get(aGame).add(theCorrelation);
            }
        }
    }
    
    public void saveAs(String theLabel) {
        // Store the statistics as a JSON object in the datastore.
        try {
            JSONObject overall = new JSONObject();
            Map<String, JSONObject> perPlayer = new HashMap<String, JSONObject>();
            Map<String, JSONObject> perGame = new HashMap<String, JSONObject>();
            
            // Store the overall statistics
            overall.put("matches", nMatches);
            overall.put("matchesFinished", nMatchesFinished);
            overall.put("matchesAbandoned", nMatchesAbandoned);
            overall.put("matchesAverageMoves", movesPerMatch.getWeightedAverage());
            overall.put("matchesAveragePlayers", playersPerMatch.getWeightedAverage());
            overall.put("matchesStatErrors", nMatchesStatErrors);
            overall.put("computeTime", nComputeFinishedAt - nComputeBeganAt);
            overall.put("computeRAM", Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory());
            overall.put("leaderboard", playerAverageScore);
            overall.put("decayedLeaderboard", playerDecayedAverageScore);
            overall.put("computedAt", System.currentTimeMillis());
            overall.put("matchesInPastHour", nMatchesInPastHour);
            overall.put("matchesInPastDay", nMatchesInPastDay);
            overall.put("matchesPerDayMedian", matchesPerDay.getMedianPerDay());
            overall.put("eloRank", theEloRank.getComputedRanks());
            overall.put("agonSkill", theAgonRank.getSkills());
            overall.put("agonScaledSkill", theAgonRank.getScaledSkills());
            overall.put("agonDifficulty", theAgonRank.getDifficulties());
            overall.put("agonScaledDifficulty", theAgonRank.getScaledDifficulties());
            overall.put("netScore", netScores);
            overall.put("statsVersion", STATS_VERSION);
            overall.put("matchesStartedChart", matchesStarted.getTimeMap());
            overall.put("observedPlayers", thePlayerNames);
            overall.put("observedGames", theGameNames);

            // Store the per-player statistics
            for (String playerName : thePlayerNames) {
                perPlayer.put(playerName, new JSONObject());
                perPlayer.get(playerName).put("averageScore", playerAverageScore.get(playerName));                
                perPlayer.get(playerName).put("decayedAverageScore", playerDecayedAverageScore.get(playerName));
                perPlayer.get(playerName).put("computedEdgeOn", computedEdgeOn.get(playerName));
                perPlayer.get(playerName).put("averageScoreOn", averageScoreOn.get(playerName));
                perPlayer.get(playerName).put("averageScoreVersus", averageScoreVersus.get(playerName));
                perPlayer.get(playerName).put("winsVersusPlayerOnGame", playerWinsVersusPlayerOnGame.get(playerName));
                perPlayer.get(playerName).put("agonSkill", theAgonRank.getSkills().get(playerName));
                perPlayer.get(playerName).put("agonScaledSkill", theAgonRank.getScaledSkills().get(playerName));
                perPlayer.get(playerName).put("eloRank", theEloRank.getComputedRanks().get(playerName));
            }

            // Store the per-game statistics
            for (String gameName : theGameNames) {
                perGame.put(gameName, new JSONObject());
                perGame.get(gameName).put("averageMoves", gameAverageMoves.get(gameName));
                perGame.get(gameName).put("agonDifficulty", theAgonRank.getDifficulties().get(gameName));
                perGame.get(gameName).put("agonScaledDifficulty", theAgonRank.getScaledDifficulties().get(gameName));
                perGame.get(gameName).put("rolePlayerAverageScore", perGameRolePlayerAverageScore.get(gameName));
                perGame.get(gameName).put("roleCorrelationWithSkill", perGameRoleCorrelationWithSkill.get(gameName));
            }

            FinalOverallStats.load(theLabel).setJSON(overall);
            for (String playerName : perPlayer.keySet()) {
                FinalPlayerStats.load(theLabel, playerName).setJSON(perPlayer.get(playerName));
            }
            for (String gameName : perGame.keySet()) {
                FinalGameStats.load(theLabel, gameName).setJSON(perGame.get(gameName));
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }        
    }    
}