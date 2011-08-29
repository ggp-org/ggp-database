package ggp.database.statistics.stored;

import ggp.database.Persistence;

import javax.jdo.annotations.*;

@PersistenceCapable
public class FinalGameStats extends StoredJSON {
    public FinalGameStats(String theLabel, String theGame) {
        super(theLabel + "_" + theGame);
    }
    
    public static FinalGameStats load(String theLabel, String theGame) {
        FinalGameStats s = Persistence.loadSpecific(theLabel + "_" + theGame, FinalGameStats.class);
        if (s == null) {
            return new FinalGameStats(theLabel, theGame);
        } else {
            return s;
        }
    }
}