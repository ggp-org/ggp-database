package ggp.database.statistics.stored;

import org.ggp.galaxy.shared.persistence.Persistence;

import javax.jdo.annotations.*;

@PersistenceCapable
public class FinalGameStats extends StoredJSON {
    public FinalGameStats(String theLabel, String theGame) {
        super(theLabel + "_game_" + theGame);
    }
    
    public static FinalGameStats load(String theLabel, String theGame) {
        FinalGameStats s = Persistence.loadSpecific(theLabel + "_game_" + theGame, FinalGameStats.class);
        if (s == null) {
            return new FinalGameStats(theLabel, theGame);
        } else {
            return s;
        }
    }
}