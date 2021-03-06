package ggp.database.statistics.stored;

import org.ggp.galaxy.shared.persistence.Persistence;

import external.JSON.JSONObject;

import java.util.Set;

import javax.jdo.annotations.*;

@PersistenceCapable
public class IntermediateStatistics extends StoredJSON {
    public IntermediateStatistics(String theLabel) {
        super(theLabel);
    }

    /* Static accessor methods */
    public static void saveIntermediateStatistics(String theLabel, JSONObject theJSON) {
        IntermediateStatistics theStats = new IntermediateStatistics(theLabel);
        theStats.setJSON(theJSON);
        theStats.save();
    }
    
    public static JSONObject loadIntermediateStatistics(String theLabel) {
        IntermediateStatistics s = Persistence.loadSpecific(theLabel, IntermediateStatistics.class);
        if (s != null) {
            return s.getJSON();
        }
        return null;
    }
    
    public static Set<IntermediateStatistics> loadAllIntermediateStatistics() {
        return Persistence.loadAll(IntermediateStatistics.class);
    }
}