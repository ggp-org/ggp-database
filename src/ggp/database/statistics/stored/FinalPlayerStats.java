package ggp.database.statistics.stored;

import ggp.database.Persistence;

import javax.jdo.annotations.*;

@PersistenceCapable
public class FinalPlayerStats extends StoredJSON {
    public FinalPlayerStats(String theLabel, String thePlayer) {
        super(theLabel + "_" + thePlayer);
    }
    
    public static FinalPlayerStats load(String theLabel, String thePlayer) {
        FinalPlayerStats s = Persistence.loadSpecific(theLabel + "_" + thePlayer, FinalPlayerStats.class);
        if (s == null) {
            return new FinalPlayerStats(theLabel, thePlayer);
        } else {
            return s;
        }
    }
}