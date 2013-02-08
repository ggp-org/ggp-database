package ggp.database.matches;

import org.ggp.galaxy.shared.persistence.Persistence;
import ggp.database.notifications.UpdateRegistry;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.annotations.*;

import com.google.appengine.api.channel.ChannelMessage;
import com.google.appengine.api.channel.ChannelService;
import com.google.appengine.api.channel.ChannelServiceFactory;
import com.google.appengine.api.datastore.Text;

import org.ggp.galaxy.shared.crypto.BaseHashing;
import org.ggp.galaxy.shared.crypto.SignableJSON;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

@PersistenceCapable
@SuppressWarnings("unused")
public class CondensedMatch {
    @PrimaryKey @Persistent public String matchURL;
    
    @Persistent public String matchId;
    @Persistent public long startTime;
    @Persistent @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true") public String randomToken;
    @Persistent public String hashedMatchHostPK;
    @Persistent public int startClock;
    @Persistent public int playClock;
    @Persistent public int moveCount;
    @Persistent public long matchLength;
    @Persistent public int matchRoles;
    @Persistent public boolean hasErrors;
    @Persistent public boolean allErrors;
    @Persistent public boolean allErrorsForSomePlayer;
    @Persistent public boolean isCompleted;
    @Persistent public Boolean isAborted;
    @Persistent public String gameMetaURL;
    @Persistent public String tournamentNameFromHost;
    @Persistent public Boolean scrambled;
    @Persistent public Double weight;
    @Persistent public Date lastUpdated;

    @Persistent public List<String> playerNamesFromHost;
    @Persistent @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true") public List<Integer> goalValues;
    @Persistent @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true") public List<Boolean> allErrorsForPlayer;
    @Persistent @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true") public List<Boolean> hasErrorsForPlayer;
    @Persistent @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true") public List<Boolean> isPlayerHuman;

    static class SignatureException extends Exception {
        private static final long serialVersionUID = 1L;
        SignatureException(String s) {
            super(s);
        }
    }
    
    private CondensedMatch(String theMatchURL, JSONObject theMatchJSON) throws JSONException, SignatureException {
        this.matchURL = theMatchURL;
        setMatchJSON(theMatchJSON);
    }
    
    public String getMatchURL() {
        return matchURL;
    }
    
    private void setMatchJSON(JSONObject theMatchJSON) throws JSONException, SignatureException {    
        if (SignableJSON.isSignedJSON(theMatchJSON)) {
            // Signed matches must have valid signatures.
            if (!SignableJSON.verifySignedJSON(theMatchJSON)) {
                throw new SignatureException("Match signature invalid!");
            }
        } else {
            // Unsigned matches must not claim they are from a particular host.
            if (theMatchJSON.has("matchHostPK")) {
                throw new SignatureException("Match has host public key, but is not signed!");
            }
        }
        theMatchJSON = condenseFullJSON(theMatchJSON);
        theMatchJSON = condenseMatchErrorsJSON(theMatchJSON);
        
        this.matchId = theMatchJSON.getString("matchId");
        this.startTime = theMatchJSON.getLong("startTime");
        this.randomToken = theMatchJSON.getString("randomToken");
        if (theMatchJSON.has("matchHostPK")) {
            this.hashedMatchHostPK = theMatchJSON.getString("matchHostPK");
        }
        this.lastUpdated = new Date();
        this.startClock = theMatchJSON.getInt("startClock");
        this.playClock = theMatchJSON.getInt("playClock");
        this.moveCount = theMatchJSON.getInt("moveCount");
        this.matchLength = theMatchJSON.getLong("matchLength");
        if (theMatchJSON.has("matchRoles")) {
            this.matchRoles = theMatchJSON.getInt("matchRoles");
        }
        if (theMatchJSON.has("hasErrors")) {
            this.hasErrors = theMatchJSON.getBoolean("hasErrors");
        }
        if (theMatchJSON.has("allErrors")) {
            this.allErrors = theMatchJSON.getBoolean("allErrors");
        }
        if (theMatchJSON.has("allErrorsForSomePlayer")) {
            this.allErrorsForSomePlayer = theMatchJSON.getBoolean("allErrorsForSomePlayer");
        }
        this.isCompleted = theMatchJSON.getBoolean("isCompleted");
        this.gameMetaURL = theMatchJSON.getString("gameMetaURL");
        if (theMatchJSON.has("isAborted")) {
        	this.isAborted = theMatchJSON.getBoolean("isAborted");
        }
        
        if (theMatchJSON.has("tournamentNameFromHost")) {
            this.tournamentNameFromHost = theMatchJSON.getString("tournamentNameFromHost");
        }
        if (theMatchJSON.has("scrambled")) {
            this.scrambled = theMatchJSON.getBoolean("scrambled");
        }
        if (theMatchJSON.has("weight")) {
            this.weight = theMatchJSON.getDouble("weight");
        }

        // per-role values
        if (theMatchJSON.has("goalValues")) {
            this.goalValues = convertToList(theMatchJSON.getJSONArray("goalValues"));
        }
        if (theMatchJSON.has("playerNamesFromHost")) {
            this.playerNamesFromHost = convertToList(theMatchJSON.getJSONArray("playerNamesFromHost"));
        }
        if (theMatchJSON.has("allErrorsForPlayer")) {
            this.allErrorsForPlayer = convertToList(theMatchJSON.getJSONArray("allErrorsForPlayer"));
        }
        if (theMatchJSON.has("hasErrorsForPlayer")) {
            this.hasErrorsForPlayer = convertToList(theMatchJSON.getJSONArray("hasErrorsForPlayer"));
        }
        if (theMatchJSON.has("isPlayerHuman")) {
        	this.isPlayerHuman = convertToList(theMatchJSON.getJSONArray("isPlayerHuman"));
        }
        
        // When the match is completed, we can phase out the channel registrations:
        // move them from persistent storage to an in-memory variable, so that we can still
        // notify subscribers about the last update.
        if (isCompleted || getIsAborted()) {
            tempClientIDs = theClientIDs;
            this.theClientIDs = null;
        }
    }
    
    private boolean getIsAborted() {
    	return isAborted != null && isAborted.booleanValue();
    }
    
    public JSONObject getMatchJSON() {
        try {
           JSONObject theMatch = new JSONObject();
           theMatch.put("matchURL", matchURL);
           
           theMatch.put("matchId", matchId);
           theMatch.put("startTime", startTime);
           theMatch.put("randomToken", randomToken);
           theMatch.put("hashedMatchHostPK", hashedMatchHostPK);
           theMatch.put("startClock", startClock);
           theMatch.put("playClock", playClock);
           theMatch.put("moveCount", moveCount);
           theMatch.put("matchLength", matchLength);
           theMatch.put("matchRoles", matchRoles);
           theMatch.put("hasErrors", hasErrors);
           theMatch.put("allErrors", allErrors);
           theMatch.put("allErrorsForSomePlayer", allErrorsForSomePlayer);
           theMatch.put("isCompleted", isCompleted);
           theMatch.put("gameMetaURL", gameMetaURL);
           theMatch.put("tournamentNameFromHost", tournamentNameFromHost);
           theMatch.put("scrambled", scrambled);
           theMatch.put("weight", weight);
           if (lastUpdated != null) {
             theMatch.put("lastUpdated", lastUpdated.getTime());
           }
           if (isAborted != null) {
        	 theMatch.put("isAborted", isAborted);
           }

           // per-role values
           if (playerNamesFromHost.size() > 0) {
             theMatch.put("playerNamesFromHost", playerNamesFromHost);
           }
           if (isPlayerHuman.size() > 0) {
        	 theMatch.put("isPlayerHuman", isPlayerHuman);
           }
           theMatch.put("allErrorsForPlayer", allErrorsForPlayer);
           theMatch.put("hasErrorsForPlayer", hasErrorsForPlayer);
           if (goalValues.size() > 0) {
             theMatch.put("goalValues", goalValues);
           }

           return theMatch;
        } catch (JSONException e) {
            return null;
        }
    }

    private void save() {
        PersistenceManager pm = Persistence.getPersistenceManager();
        pm.makePersistent(this);
        pm.close();
    }

    /* Condense JSON for statistics computations */
    private static JSONObject condenseFullJSON(JSONObject theJSON) throws JSONException {
        theJSON.put("moveCount", theJSON.getJSONArray("moves").length());
        if (theJSON.getJSONArray("moves").length() > 0) {
            theJSON.put("matchRoles", theJSON.getJSONArray("moves").getJSONArray(0).length());
        } else if (theJSON.has("errors") && theJSON.getJSONArray("errors").length() > 0) {
            theJSON.put("matchRoles", theJSON.getJSONArray("errors").getJSONArray(0).length());
        } else if (theJSON.has("playerNamesFromHost") && theJSON.getJSONArray("playerNamesFromHost").length() > 0) {
            theJSON.put("matchRoles", theJSON.getJSONArray("playerNamesFromHost").length());
        } else {
            // Whatever, don't set "matchRoles" yet.
        }
        theJSON.put("matchLength", theJSON.getJSONArray("stateTimes").getLong(theJSON.getJSONArray("stateTimes").length()-1)-theJSON.getJSONArray("stateTimes").getLong(0));
        if (theJSON.has("matchHostPK")) {
          theJSON.put("matchHostPK", BaseHashing.computeSHA1Hash(theJSON.getString("matchHostPK")));
        }
        theJSON.remove("matchHostSignature");
        theJSON.remove("states");      // Strip out all of the large fields
        theJSON.remove("moves");       // that we won't need most of the time.        
        theJSON.remove("stateTimes");  // This is why we can store it here.        
        return theJSON;
    }
    
    private static JSONObject condenseMatchErrorsJSON(JSONObject theJSON) throws JSONException {
        if (!theJSON.has("errors")) return theJSON;

        int nRoles = -1;
        if (theJSON.has("matchRoles")) {
            nRoles = theJSON.getInt("matchRoles");
        }
        boolean allErrors = true;
        boolean hasErrors = false;
        boolean allErrorsForSomePlayer = false;
        List<Boolean> allErrorsForPlayer = new ArrayList<Boolean>();
        List<Boolean> hasErrorsForPlayer = new ArrayList<Boolean>();
        for (int i = 0; i < nRoles; i++) {
            allErrorsForPlayer.add(true);
            hasErrorsForPlayer.add(false);
        }
        boolean noErrorCandidates = true;
        
        JSONArray theErrors = theJSON.getJSONArray("errors");
        for (int i = 0; i < theErrors.length(); i++) {
            JSONArray theErrorEntry = theErrors.getJSONArray(i);
            for (int j = 0; j < theErrorEntry.length(); j++) {
                if (theErrorEntry.getString(j).isEmpty()) {
                    allErrors = false;
                    allErrorsForPlayer.set(j, false);
                } else {
                    hasErrors = true;
                    hasErrorsForPlayer.set(j, true);
                }
                noErrorCandidates = false;
            }
        }
        if (noErrorCandidates) {
          // If there are no moves so far, technically "all moves have been errors",
          // but that's a confusing way to present things, so it's better to show the
          // equally-true information "all moves have been error-free".
          allErrors = false;
          for (int i = 0; i < nRoles; i++) {
            allErrorsForPlayer.set(i, false);
          }
        }
        for (int i = 0; i < nRoles; i++) {
          if (allErrorsForPlayer.get(i)) {
            allErrorsForSomePlayer = true;
          }
        }
        
        theJSON.put("allErrors", allErrors);
        theJSON.put("hasErrors", hasErrors);
        theJSON.put("allErrorsForSomePlayer", allErrorsForSomePlayer);
        theJSON.put("hasErrorsForPlayer", hasErrorsForPlayer);
        theJSON.put("allErrorsForPlayer", allErrorsForPlayer);
        theJSON.remove("errors");
        return theJSON;
    }
    
    /* Channel registration methods */
    
    // When we finish a match, we want to clear the persistent client ID list.
    // However we still need to send out one final ping, so we keep a temporary
    // version of the list in-memory until after that final ping is sent.
    private Set<String> tempClientIDs;
    @Persistent private Set<String> theClientIDs;    
    
    public boolean addClientID(String clientID) {
        if (isCompleted) return false;
        if (getIsAborted()) return false;
        if (theClientIDs == null) theClientIDs = new HashSet<String>();
        theClientIDs.add(clientID);
        return true;
    }

    public int numClientIDs() {
        if (theClientIDs == null) return 0;
        return theClientIDs.size();
    }

    public void pingChannelClients() {
        if (theClientIDs == null && tempClientIDs == null) return;
        ChannelService chanserv = ChannelServiceFactory.getChannelService();
        if (theClientIDs != null) {
            for(String clientID : theClientIDs) {
                chanserv.sendMessage(new ChannelMessage(clientID, matchURL));
            }
        }
        if (tempClientIDs != null) {
            for(String clientID : tempClientIDs) {
                chanserv.sendMessage(new ChannelMessage(clientID, matchURL));
            }            
        }
    }    

    /* Static accessor methods */
    public static CondensedMatch storeCondensedMatchJSON(String matchURL, JSONObject theMatchJSON) throws IOException {        
        try {
            boolean changedActiveSet = false;
            CondensedMatch theMatchData = Persistence.loadSpecific(matchURL, CondensedMatch.class);
            if (theMatchData == null) {
                theMatchData = new CondensedMatch(matchURL, theMatchJSON);
            } else {
                changedActiveSet = !theMatchData.isCompleted && !theMatchData.getIsAborted();
                theMatchData.setMatchJSON(theMatchJSON);
                if (!theMatchData.isCompleted && !theMatchData.getIsAborted()) changedActiveSet = false;
            }
            theMatchData.save();
            // TODO: handle pings in a separate task queue?
            theMatchData.pingChannelClients();
            if (theMatchData.hashedMatchHostPK != null && !theMatchData.hashedMatchHostPK.isEmpty()) {
                UpdateRegistry.pingClients("query/filter,recent," + theMatchData.hashedMatchHostPK);
                UpdateRegistry.pingClients("query/filterGame,recent," + theMatchData.hashedMatchHostPK + "," + theMatchData.gameMetaURL);
                if (changedActiveSet) {
                    UpdateRegistry.pingClients("query/filterActiveSet,recent," + theMatchData.hashedMatchHostPK);
                }
                if (theMatchData.playerNamesFromHost != null) {
                    for (String playerName : theMatchData.playerNamesFromHost) {
                    	if (!playerName.isEmpty()) {
                    		UpdateRegistry.pingClients("query/filterPlayer,recent," + theMatchData.hashedMatchHostPK + "," + playerName);
                    	}
                    }
                }
            }
            return theMatchData;
        } catch (SignatureException se) {
            throw new IOException(se); // Match not signed: ignore it.
        } catch (JSONException je) {
            throw new IOException(je); // Match JSON not valid: ignore it;
        }
    }

    public static JSONObject loadCondensedMatchJSON(String matchURL) throws IOException {
        CondensedMatch c = Persistence.loadSpecific(matchURL, CondensedMatch.class);
        if (c == null) return null;
        return c.getMatchJSON();
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> convertToList(JSONArray x) throws JSONException {
        List<T> y = new ArrayList<T>();
        for (int i = 0; i < x.length(); i++) {
            y.add((T)x.get(i));
        }
        return y;
    }    
}