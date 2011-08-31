package ggp.database.statistics.counters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AgonRank takes in completed matches and computes the "Agon skill" of the
 * participating players and the "Agon difficulty" of the games played. This
 * involves doing a standard Elo ranking of the players, except including each
 * game as an additional "player" for the match. Thus, when a single player is
 * solving a puzzle, their opponent is the puzzle; when two players are playing
 * a game of tic-tac-toe, they are also each playing against the game itself.
 * This approach cleanly handles a number of tricky GGP situations that other
 * ranking systems handle less well, and also generates both skill levels for
 * the players and also a difficulty metric for the games.
 * 
 * To be more precise, we treat each <game,role> pair as a distinct game when
 * performing this ranking, since roles can have different difficulties.
 * 
 * This ranking function was first checked in on July 4th, 2011.
 * More details about this ranking function will be forthcoming.
 * 
 * @author Sam Schreiber
 */
public class AgonRank {
    private EloRank theInternalRank;
    
    private boolean dirty = true;
    private Map<String,Double> theSkills;
    private Map<String,List<Double>> theDifficulties;
    private Map<String,Double> theScaledSkills;
    private Map<String,List<Double>> theScaledDifficulties;    
    
    public AgonRank() {
        theInternalRank = new EloRank();
    }
    
    public void addNextMatch(String aPlayer, String bPlayer, int aScore, int bScore) {
        theInternalRank.addNextMatch(aPlayer, bPlayer, aScore, bScore);
        dirty = true;
    }
    
    public void addMatchParticipation(String aPlayer, String theGame, int theRole, int aScore) {
        theInternalRank.addNextMatch(aPlayer, "game:" + theGame + ".role" + theRole, aScore, 100-aScore);
        dirty = true;
    }
    
    public double getComputedSkill(String aPlayer) {
        return theInternalRank.getComputedRanks().get(aPlayer);
    }
    
    public void computeResultsWhenNeeded() {
        if (!dirty)
            return;
        double minSkill = Double.POSITIVE_INFINITY;
        double maxSkill = Double.NEGATIVE_INFINITY;
        double minDifficulty = Double.POSITIVE_INFINITY;
        double maxDifficulty = Double.NEGATIVE_INFINITY;
        
        theSkills = new HashMap<String,Double>(); 
        theDifficulties = new HashMap<String,List<Double>>();
        Map<String,Double> theInternalRanks = theInternalRank.getComputedRanks();
        for (String key : theInternalRanks.keySet()) {
            if (key.startsWith("game:")) {
                String theGameRole = key.replace("game:", "");
                String theGame = theGameRole.substring(0, theGameRole.lastIndexOf(".role"));
                int theRole = Integer.parseInt(theGameRole.substring(theGameRole.lastIndexOf(".role")+5));
                if (!theDifficulties.containsKey(theGame)) {
                    theDifficulties.put(theGame, new ArrayList<Double>());
                }
                while (theDifficulties.get(theGame).size() < theRole+1) {
                    theDifficulties.get(theGame).add(0.0);
                }
                double theSkill = theInternalRanks.get(key);
                minDifficulty = Math.min(minDifficulty, theSkill);
                maxDifficulty = Math.max(maxDifficulty, theSkill);
                theDifficulties.get(theGame).set(theRole, theSkill);                
            } else {
                double theSkill = theInternalRanks.get(key);
                minSkill = Math.min(minSkill, theSkill);
                maxSkill = Math.max(maxSkill, theSkill);
                theSkills.put(key, theSkill);                
            }
        }
        theScaledSkills = new HashMap<String,Double>();
        theScaledDifficulties = new HashMap<String,List<Double>>();
        for (String key : theSkills.keySet()) {
            double theValue = theSkills.get(key);
            theScaledSkills.put(key, (theValue-minSkill)/(maxSkill-minSkill));
        }
        for (String key : theDifficulties.keySet()) {
            List<Double> theValues = new ArrayList<Double>(theDifficulties.get(key));
            for (int i = 0; i < theValues.size(); i++) {
                double theValue = theValues.get(i);
                theValues.set(i, (theValue-minDifficulty)/(maxDifficulty-minDifficulty));
            }
            theScaledDifficulties.put(key, theValues);
        }
        dirty = false;
    }
    
    public Map<String,Double> getSkills() {
        computeResultsWhenNeeded();
        return theSkills;
    }
    
    public Map<String,Double> getScaledSkills() {
        computeResultsWhenNeeded();
        return theScaledSkills;        
    }
    
    public Map<String,List<Double>> getDifficulties() {
        computeResultsWhenNeeded();
        return theDifficulties;
    }
    
    public Map<String,List<Double>> getScaledDifficulties() {
        computeResultsWhenNeeded();
        return theScaledDifficulties;
    }        
}