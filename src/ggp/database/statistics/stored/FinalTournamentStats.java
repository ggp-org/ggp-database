package ggp.database.statistics.stored;

import org.ggp.galaxy.shared.persistence.Persistence;

import javax.jdo.annotations.*;

@PersistenceCapable
public class FinalTournamentStats extends StoredJSON {
    public FinalTournamentStats(String theLabel, String theTournament) {
        super(theLabel + "_tournament_" + theTournament);
    }
    
    public static FinalTournamentStats load(String theLabel, String theTournament) {
        FinalTournamentStats s = Persistence.loadSpecific(theLabel + "_tournament_" + theTournament, FinalTournamentStats.class);
        if (s == null) {
            return new FinalTournamentStats(theLabel, theTournament);
        } else {
            return s;
        }
    }
}