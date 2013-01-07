package ggp.database.logs;

import org.ggp.shared.persistence.Persistence;
import ggp.database.matches.CondensedMatch;
import ggp.database.notifications.UpdateRegistry;
import ggp.database.statistics.StringCompressor;
import ggp.database.statistics.stored.FinalGameStats;
import ggp.database.statistics.stored.FinalOverallStats;
import ggp.database.statistics.stored.FinalPlayerStats;
import ggp.database.statistics.stored.IntermediateStatistics;

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
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.channel.ChannelMessage;
import com.google.appengine.api.channel.ChannelService;
import com.google.appengine.api.channel.ChannelServiceFactory;
import com.google.appengine.api.datastore.Text;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

@PersistenceCapable
@SuppressWarnings("unused")
public class MatchLog {
    @PrimaryKey @Persistent public String exponentKey;
    
    @Persistent public String playerName;
    @Persistent public String matchURL;    
    @Persistent public Text theData;
    
    public static void respondWithLog(HttpServletResponse resp, String theLogURL) throws IOException {
        JSONObject theResponse = null;
        {
            String[] splitLogURL = theLogURL.split("/");
            theResponse = loadLogData(splitLogURL[1], "http://matches.ggp.org/matches/" + splitLogURL[0] + "/");
        }
        if (theResponse != null) {
            resp.getWriter().println(theResponse.toString());
        } else {
            resp.setStatus(404);
        }            
    }
    
    public MatchLog(String playerName, String theMatchURL, JSONObject theData) throws JSONException {
        this.exponentKey = getKey(playerName, theMatchURL);
        this.playerName = playerName;
        this.matchURL = theMatchURL;
        this.theData = new Text(StringCompressor.compress(theData.toString()));
        save();
    }
    
    public String getPlayerName() {
        return playerName;
    }
    
    public String getMatchURL() {
        return matchURL;
    }
    
    public JSONObject getLogData() {
        try {
            return new JSONObject(StringCompressor.decompress(theData.getValue()));
        } catch (JSONException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    private void save() {
        PersistenceManager pm = Persistence.getPersistenceManager();
        pm.makePersistent(this);
        pm.close();
    }
    
    public static JSONObject loadLogData(String playerName, String matchURL) throws IOException {
        MatchLog m = Persistence.loadSpecific(getKey(playerName, matchURL), MatchLog.class);
        if (m == null) return null;
        return m.getLogData();
    }
    
    private static String getKey(String playerName, String matchURL) {
        return playerName + "_" + matchURL;
    }
}