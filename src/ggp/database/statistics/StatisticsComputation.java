package ggp.database.statistics;

import ggp.database.Persistence;
import ggp.database.matches.CondensedMatch;
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

import javax.jdo.Query;

import org.datanucleus.store.appengine.query.JDOCursorHelper;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

public class StatisticsComputation {
    public static final int STATS_VERSION = 9;

    public static String getHostName(String matchHostPK) {
        if (matchHostPK.contains("0ca7065b86d7646166d86233f9e23ac47d8320d4")) return "SimpleGameSim";
        if (matchHostPK.contains("90bd08a7df7b8113a45f1e537c1853c3974006b2")) return "Apollo";
        if (matchHostPK.contains("f69721b2f73839e513eed991e96824f1af218ac1")) return "Dresden";
        return matchHostPK;
    }

    @SuppressWarnings("unchecked")
    public static void computeStatistics() throws IOException {
        Map<String, StatisticsComputation> statsForLabel = new HashMap<String, StatisticsComputation>();
        statsForLabel.put("all", new StatisticsComputation());
        statsForLabel.put("unsigned", new StatisticsComputation());
        
        {
            /* Compute the statistics in a single sorted pass over the data */
            String cursorForContinuation = "";
            while(true) {
                Query query = Persistence.getPersistenceManager().newQuery(CondensedMatch.class);
                query.setRange(0,1000);
                query.getFetchPlan().setFetchSize(100);
                query.setOrdering("startTime");                
                if (!cursorForContinuation.isEmpty()) {
                    Cursor cursor = Cursor.fromWebSafeString(cursorForContinuation);
                    Map<String, Object> extensionMap = new HashMap<String, Object>();
                    extensionMap.put(JDOCursorHelper.CURSOR_EXTENSION, cursor);                    
                    query.setExtensions(extensionMap);
                }
                List<CondensedMatch> results = (List<CondensedMatch>) query.execute();
                if (results.isEmpty())
                    break;
                                
                for (CondensedMatch match : results) {
                    statsForLabel.get("all").add(match, true);
                    if (match.hashedMatchHostPK != null && !match.hashedMatchHostPK.isEmpty()) {
                      if (!statsForLabel.containsKey(match.hashedMatchHostPK)) {
                        statsForLabel.put(match.hashedMatchHostPK, new StatisticsComputation());
                      }
                      statsForLabel.get(match.hashedMatchHostPK).add(match, false);
                    } else {
                      statsForLabel.get("unsigned").add(match, true);
                    }
                }
                Cursor cursor = JDOCursorHelper.getCursor(results);
                cursorForContinuation = cursor.toWebSafeString();
            }
        }
        
        // Save all of the statistics
        for (String theLabel : statsForLabel.keySet()) {
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
    Map<String,Map<String,WeightedAverage>> averageScoreOn = new HashMap<String,Map<String,WeightedAverage>>();
    Map<String,Map<String,WeightedAverage>> averageScoreVersus = new HashMap<String,Map<String,WeightedAverage>>();
    Map<String,WeightedAverage> gameAverageMoves = new HashMap<String,WeightedAverage>();
    Map<String,Map<String,Map<String,WinLossCounter>>> playerWinsVersusPlayerOnGame = new HashMap<String,Map<String,Map<String,WinLossCounter>>>();
    Map<String,Double> netScores = new HashMap<String,Double>();

    Set<String> theGameNames = new HashSet<String>();
    Set<String> thePlayerNames = new HashSet<String>();
    WeightedAverage playersPerMatch = new WeightedAverage();
    WeightedAverage movesPerMatch = new WeightedAverage();
    EloRank theEloRank = new EloRank();
    AgonRank theAgonRank = new AgonRank();
    
    TimeBucketSeries matchesStarted = new TimeBucketSeries();

    long nComputeBeganAt = System.currentTimeMillis();

    public void add(CondensedMatch theMatch, boolean addHostToPlayerNames) {
        nMatches++;
        matchesPerDay.addToDay(1, theMatch.startTime);
        if (System.currentTimeMillis() - theMatch.startTime < 3600000L) nMatchesInPastHour++;
        if (System.currentTimeMillis() - theMatch.startTime < 86400000L) nMatchesInPastDay++;
        
        // TODO: Use a better way to determine the number of players in the match,
        // that works for matches that haven't been signed.
        if (theMatch.playerNamesFromHost != null) {
            playersPerMatch.addValue(theMatch.playerNamesFromHost.size());
        }
        
        if (theMatch.isCompleted) {
            nMatchesFinished++;
            movesPerMatch.addValue(theMatch.moveCount);

            String theGame = theMatch.gameMetaURL;
            if (!gameAverageMoves.containsKey(theGame)) {
                gameAverageMoves.put(theGame, new WeightedAverage());
            }
            gameAverageMoves.get(theGame).addValue(theMatch.moveCount);
            theGameNames.add(theGame);

            boolean matchHadErrors = theMatch.hasErrors;                
            matchesStarted.addEntry(theMatch.startTime);

            // Only compute score-related statistics for matches with named players
            // within a specified player namespace.
            if (theMatch.playerNamesFromHost != null && theMatch.hashedMatchHostPK != null && !theMatch.hashedMatchHostPK.isEmpty()) {
                List<String> matchPlayers = new ArrayList<String>();
                for (int i = 0; i < theMatch.playerNamesFromHost.size(); i++) {
                    matchPlayers.add( (addHostToPlayerNames ? getHostName(theMatch.hashedMatchHostPK)+"." : "") + theMatch.playerNamesFromHost.get(i));
                }
                thePlayerNames.addAll(matchPlayers);

                // Score-related statistics.                        
                for (int i = 0; i < matchPlayers.size(); i++) {
                    String aPlayer = matchPlayers.get(i);
                    int aPlayerScore = theMatch.goalValues.get(i);

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

                    double ageInDays = (double)(System.currentTimeMillis() - theMatch.startTime) / (double)(86400000L);
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
                        int bPlayerScore = theMatch.goalValues.get(j);
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
                        }
                    }
                    
                    theAgonRank.addMatchParticipation(aPlayer, theGame, i, aPlayerScore);
                }
            }
        } else {
            nMatchesAbandoned++;
        }        
    }
    
    public void saveAs(String theLabel) {
        /* Finally we're done with computing statistics */
        long nComputeTime = System.currentTimeMillis() - nComputeBeganAt;

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
            overall.put("computeTime", nComputeTime);
            overall.put("computeRAM", Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory());
            overall.put("leaderboard", playerAverageScore);
            overall.put("decayedLeaderboard", playerDecayedAverageScore);
            overall.put("computedAt", System.currentTimeMillis());
            overall.put("matchesInPastHour", nMatchesInPastHour);
            overall.put("matchesInPastDay", nMatchesInPastDay);
            overall.put("matchesPerDayMedian", matchesPerDay.getMedianPerDay());
            overall.put("eloRank", theEloRank.getComputedRanks());
            overall.put("agonSkill", theAgonRank.getSkills());
            overall.put("agonDifficulty", theAgonRank.getDifficulties());
            overall.put("netScore", netScores);
            overall.put("statsVersion", STATS_VERSION);
            overall.put("matchesStartedChart", matchesStarted.getTimeMap());

            // Store the per-player statistics
            for (String playerName : thePlayerNames) {
                perPlayer.put(playerName, new JSONObject());
                perPlayer.get(playerName).put("averageScore", playerAverageScore.get(playerName));                
                perPlayer.get(playerName).put("decayedAverageScore", playerDecayedAverageScore.get(playerName));
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