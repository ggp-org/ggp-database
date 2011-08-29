package ggp.database;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.annotations.*;

import com.google.appengine.api.datastore.Text;

@PersistenceCapable
public class ServerState {
    @SuppressWarnings("unused")
    @PrimaryKey @Persistent private String thePrimaryKey;
    @Persistent private Text mostRecentUpdate;
    @Persistent private Date mostRecentUpdateWhen;
    @Persistent private List<Date> mostRecentUpdateTimes;
    @Persistent private String theValidationToken;

    private static final int UPDATE_TIMES_TO_STORE = 100;

    private ServerState() {
        thePrimaryKey = "ServerState";
        mostRecentUpdateTimes = new ArrayList<Date>();
        theValidationToken = getRandomString(32);
    }

    public String getValidationToken() {
        if (theValidationToken == null) return "";
        return theValidationToken;
    }

    public void rotateValidationToken() {
        theValidationToken = getRandomString(32);
    }

    public void addUpdate(String theUpdate) {
        mostRecentUpdate = new Text(theUpdate);
        mostRecentUpdateWhen = new Date();
        mostRecentUpdateTimes.add(new Date());
        if (mostRecentUpdateTimes.size() > UPDATE_TIMES_TO_STORE)
            mostRecentUpdateTimes.remove(0);
    }

    public String getMostRecentUpdate() {
        if (mostRecentUpdate == null) return null;
        return mostRecentUpdate.getValue();
    }

    public Date getMostRecentUpdateWhen() {
        if (mostRecentUpdateWhen == null) return null;
        return mostRecentUpdateWhen;
    }

    public List<Date> getUpdateTimes() {
        return mostRecentUpdateTimes;
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