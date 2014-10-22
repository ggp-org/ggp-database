package ggp.database;

import java.io.IOException;
import java.util.Random;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.annotations.*;

import org.ggp.galaxy.shared.persistence.Persistence;

@PersistenceCapable
public class ServerState {
    @PrimaryKey @Persistent private String thePrimaryKey;
    @Persistent private String theValidationToken;

    private ServerState() {
        thePrimaryKey = "ServerState";
        theValidationToken = getRandomString(32);
    }

    public String getValidationToken() {
        if (theValidationToken == null) return "";
        return theValidationToken;
    }

    public void rotateValidationToken() {
        theValidationToken = getRandomString(32);
    }

    public void save() {
        PersistenceManager pm = Persistence.getPersistenceManager();
        pm.makePersistent(this);
        pm.close();
    }

    /* Static accessor methods */
    public static ServerState loadState() throws IOException {
        Set<ServerState> theStates = Persistence.loadAll(ServerState.class);
        if (theStates.size() > 0) {
            return theStates.iterator().next();
        } else {
            return new ServerState();
        }
   }

    public static String getRandomString(int nLength) {
        Random theGenerator = new Random();
        String theString = "";
        for (int i = 0; i < nLength; i++) {
            int nVal = theGenerator.nextInt(62);
            if (nVal < 26) theString += (char)('a' + nVal);
            else if (nVal < 52) theString += (char)('A' + (nVal-26));
            else if (nVal < 62) theString += (char)('0' + (nVal-52));
        }
        return theString;
    }
}