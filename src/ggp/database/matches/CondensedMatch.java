package ggp.database.matches;

import ggp.database.Persistence;
import ggp.database.notifications.UpdateRegistry;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.annotations.*;

import util.crypto.SignableJSON;

import com.google.appengine.api.channel.ChannelMessage;
import com.google.appengine.api.channel.ChannelService;
import com.google.appengine.api.channel.ChannelServiceFactory;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.repackaged.org.json.JSONArray;
import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

@PersistenceCapable
@SuppressWarnings("unused")
public class CondensedMatch {
    @PrimaryKey @Persistent private String matchURL;
    
    @Persistent private String matchId;
    @Persistent private long startTime;
    @Persistent @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true") private String randomToken;
    @Persistent private String hashedMatchHostPK;
    @Persistent private int startClock;
    @Persistent private int playClock;
    @Persistent private int moveCount;
    @Persistent private long matchLength;
    @Persistent private int matchRoles;
    @Persistent private boolean hasErrors;
    @Persistent private boolean allErrors;
    @Persistent private boolean allErrorsForSomePlayer;
    @Persistent private boolean isCompleted;
    @Persistent private String gameMetaURL;
    @Persistent private String gameName; // TODO: remove this
    @Persistent private String tournamentNameFromHost;
    @Persistent private Boolean scrambled;
    @Persistent private Double weight;

    @Persistent private List<String> playerNamesFromHost;
    @Persistent @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true") private List<Integer> goalValues;
    @Persistent @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true") private List<String> gameRoleNames; // TODO: remove this
    @Persistent @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true") private List<Boolean> allErrorsForPlayer;
    @Persistent @Extension(vendorName = "datanucleus", key = "gae.unindexed", value="true") private List<Boolean> hasErrorsForPlayer;

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
        if (theMatchJSON.has("gameName")) {
            this.gameName = theMatchJSON.getString("gameName");
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
        if (theMatchJSON.has("gameRoleNames")) {
            this.gameRoleNames = convertToList(theMatchJSON.getJSONArray("gameRoleNames"));
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
        
        // When the match is compelted, we can phase out the channel registrations:
        // move them from persistent storage to an in-memory variable, so that we can still
        // notify subscribers about the last update.
        if (isCompleted) {
            tempClientIDs = theClientIDs;
            this.theClientIDs = null;
        }
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
           theMatch.put("gameName", gameName);
           theMatch.put("tournamentNameFromHost", tournamentNameFromHost);
           theMatch.put("scrambled", scrambled);
           theMatch.put("weight", weight);
           
           // per-role values
           theMatch.put("gameRoleNames", gameRoleNames);
           theMatch.put("playerNamesFromHost", playerNamesFromHost);
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
        } else {
            // Whatever, don't set "matchRoles" yet, we won't use it anyway.
        }
        theJSON.put("matchLength", theJSON.getJSONArray("stateTimes").getLong(theJSON.getJSONArray("stateTimes").length()-1)-theJSON.getJSONArray("stateTimes").getLong(0));
        if (theJSON.has("matchHostPK")) {
          theJSON.put("matchHostPK", computeHash(theJSON.getString("matchHostPK")));
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
    public static void storeCondensedMatchJSON(String matchURL, JSONObject theMatchJSON) throws IOException {        
        try {
            boolean changedActiveSet = false;
            CondensedMatch theMatchData = Persistence.loadSpecific(matchURL, CondensedMatch.class);
            if (theMatchData == null) {
                theMatchData = new CondensedMatch(matchURL, theMatchJSON);
            } else {
                changedActiveSet = !theMatchData.isCompleted;
                theMatchData.setMatchJSON(theMatchJSON);
                if (!theMatchData.isCompleted) changedActiveSet = false;
            }
            theMatchData.save();
            theMatchData.pingChannelClients();
            if (!theMatchData.hashedMatchHostPK.isEmpty()) {
                UpdateRegistry.pingClients("query/filter,recent," + theMatchData.hashedMatchHostPK);
                UpdateRegistry.pingClients("query/filterGame,recent," + theMatchData.hashedMatchHostPK + "," + theMatchData.gameMetaURL);
                if (changedActiveSet) {
                    UpdateRegistry.pingClients("query/filterActiveSet,recent," + theMatchData.hashedMatchHostPK);
                }
                if (theMatchData.playerNamesFromHost != null) {
                    for (String playerName : theMatchData.playerNamesFromHost) {
                        UpdateRegistry.pingClients("query/filterPlayer,recent," + theMatchData.hashedMatchHostPK + "," + playerName);
                    }
                }
            }
        } catch (SignatureException se) {
            // Match not signed: ignore it.
        } catch (JSONException je) {
            throw new IOException(je); // Match JSON not valid: ignore it;
        }
    }

    public static JSONObject loadCondensedMatchJSON(String matchURL) throws IOException {
        CondensedMatch c = Persistence.loadSpecific(matchURL, CondensedMatch.class);
        if (c == null) return null;
        return c.getMatchJSON();
    }

    // Computes the SHA1 hash of a given input string, and represents
    // that hash as a hexadecimal string.
    private static String computeHash(String theData) {
        try {
            MessageDigest SHA1 = MessageDigest.getInstance("SHA1");

            DigestInputStream theDigestStream = new DigestInputStream(
                    new BufferedInputStream(new ByteArrayInputStream(
                            theData.getBytes("UTF-8"))), SHA1);
            while (theDigestStream.read() != -1);
            byte[] theHash = SHA1.digest();

            Formatter hexFormat = new Formatter();
            for (byte x : theHash) {
                hexFormat.format("%02x", x);
            }
            return hexFormat.toString();
        } catch (Exception e) {
            return null;
        }
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