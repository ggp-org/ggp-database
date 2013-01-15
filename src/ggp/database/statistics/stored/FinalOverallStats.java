package ggp.database.statistics.stored;

import org.ggp.galaxy.shared.persistence.Persistence;

import javax.jdo.annotations.PersistenceCapable;

@PersistenceCapable
public class FinalOverallStats extends StoredJSON {
    public FinalOverallStats(String theLabel) {
        super(theLabel);
    }
    
    public static FinalOverallStats load(String theLabel) {
        FinalOverallStats s = Persistence.loadSpecific(theLabel, FinalOverallStats.class);
        if (s == null) {
            return new FinalOverallStats(theLabel);
        } else {
            return s;
        }
    }
}