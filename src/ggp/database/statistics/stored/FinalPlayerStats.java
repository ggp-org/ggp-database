package ggp.database.statistics.stored;

import org.ggp.galaxy.shared.persistence.Persistence;

import javax.jdo.annotations.*;

@PersistenceCapable
public class FinalPlayerStats extends StoredJSON {
    public FinalPlayerStats(String theLabel, String thePlayer) {
        super(theLabel + "_player_" + thePlayer);
    }
    
    public static FinalPlayerStats load(String theLabel, String thePlayer) {
        FinalPlayerStats s = Persistence.loadSpecific(theLabel + "_player_" + thePlayer, FinalPlayerStats.class);
        if (s == null) {
            return new FinalPlayerStats(theLabel, thePlayer);
        } else {
            return s;
        }
    }
}