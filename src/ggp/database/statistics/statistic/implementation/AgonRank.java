package ggp.database.statistics.statistic.implementation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.appengine.repackaged.org.json.JSONException;

import ggp.database.statistics.statistic.CounterStatistic;

public class AgonRank extends EloRank {
    protected void updateMatchParticipation(CounterStatistic.NaiveCounter aPlayer, String theGame, int theRole, double aScore) {
        dirty = true;
        updateRank(aPlayer, getPerPlayerStatistic("game:" + theGame + ".role" + theRole), aScore, 100-aScore);
    }

    private boolean dirty = true;
    private Map<String,Double> theSkills;
    private Map<String,List<Double>> theDifficulties;
    private Map<String,Double> theScaledSkills;
    private Map<String,List<Double>> theScaledDifficulties;

    public void computeResultsWhenNeeded() {
        if (!dirty)
            return;
        double minSkill = Double.POSITIVE_INFINITY;
        double maxSkill = Double.NEGATIVE_INFINITY;
        double minDifficulty = Double.POSITIVE_INFINITY;
        double maxDifficulty = Double.NEGATIVE_INFINITY;
        
        theSkills = new HashMap<String,Double>();
        theDifficulties = new HashMap<String,List<Double>>();
        for (String key : getKnownPlayerNames()) {
            double theSkill = getPerPlayerStatistic(key).getValue();
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
                minDifficulty = Math.min(minDifficulty, theSkill);
                maxDifficulty = Math.max(maxDifficulty, theSkill);
                theDifficulties.get(theGame).set(theRole, theSkill);                
            } else {
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
    
    @Override
    public Set<FinalForm> getFinalForms() throws JSONException {
        computeResultsWhenNeeded();
        HashSet<FinalForm> theForms = new HashSet<FinalForm>();
        theForms.add(new FinalForm("agonSkill", theSkills));
        theForms.add(new FinalForm("agonDifficulty", theDifficulties));
        theForms.add(new FinalForm("agonScaledSkill", theScaledSkills));
        theForms.add(new FinalForm("agonScaledDifficulty", theScaledDifficulties));
        return theForms;
    }
    @Override
    public Set<FinalForm> getPerGameFinalForms(String forGame) throws JSONException {
        computeResultsWhenNeeded();
        HashSet<FinalForm> theForms = new HashSet<FinalForm>();
        theForms.add(new FinalForm("agonDifficulty", theDifficulties.get(forGame)));
        theForms.add(new FinalForm("agonScaledDifficulty", theScaledDifficulties.get(forGame)));
        return theForms;
    }
    @Override
    public Set<FinalForm> getPerPlayerFinalForms(String forPlayer) throws JSONException {
        computeResultsWhenNeeded();
        HashSet<FinalForm> theForms = new HashSet<FinalForm>();
        theForms.add(new FinalForm("agonSkill", theSkills.get(forPlayer)));
        theForms.add(new FinalForm("agonScaledSkill", theScaledSkills.get(forPlayer)));
        return theForms;
    }    
}