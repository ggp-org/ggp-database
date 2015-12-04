package ggp.database.queries;

import org.ggp.galaxy.shared.persistence.Persistence;

import external.JSON.JSONArray;
import external.JSON.JSONException;
import external.JSON.JSONObject;
import ggp.database.logs.MatchLog;
import ggp.database.matches.CondensedMatch;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.jdo.Query;
import javax.servlet.http.HttpServletResponse;

import org.datanucleus.store.appengine.query.JDOCursorHelper;

import com.google.appengine.api.datastore.Cursor;

public class MatchQuery {
    @SuppressWarnings("unchecked")
    public static void respondToQuery(HttpServletResponse resp, String theRPC) throws IOException {
        JSONObject theResponse = null;
        if (theRPC.startsWith("filterLog")) {
        	theResponse = runLogQuery(resp, theRPC);
        } else if (theRPC.startsWith("export1000")) {
        	theResponse = runExportQuery(resp, theRPC);
        } else if (theRPC.startsWith("filter")) {
            Deque<String> theSplitParams = new ArrayDeque<String>(Arrays.asList(theRPC.split(",")));
            String theVerb, theDomain, theHost;
            try {
                theVerb = theSplitParams.pop();
                theDomain = theSplitParams.pop();
                theHost = theSplitParams.pop();
            } catch (NoSuchElementException e) {
                resp.setStatus(404);
                return;            	
            } catch (ArrayIndexOutOfBoundsException e) {
                resp.setStatus(404);
                return;
            }
            
            if (theVerb.isEmpty() || theDomain.isEmpty() || theHost.isEmpty()) {
                resp.setStatus(404);
                return;
            }
            Query query = Persistence.getPersistenceManager().newQuery(CondensedMatch.class);
            String queryFilter = "";
            if (theVerb.equals("filterPlayer")) {
                String thePlayer = theSplitParams.pop();
                if (thePlayer.isEmpty()) {
                    resp.setStatus(404);
                    return;
                }
                queryFilter += "playerNamesFromHost == '" + thePlayer + "'";
            } else if (theVerb.equals("filterGame")) {
                String theGame = theSplitParams.pop();
                if (theGame.isEmpty()) {
                    resp.setStatus(404);
                    return;
                }
                queryFilter += "gameMetaURL == '" + theGame + "'";
            } else if (theVerb.equals("filterTournament")) {
                String theTournament = theSplitParams.pop();
                if (theTournament.isEmpty()) {
                    resp.setStatus(404);
                    return;
                }
                queryFilter += "tournamentNameFromHost == '" + theTournament + "'";            	
            } else if (theVerb.equals("filterActiveSet")) {
                String sixHoursAgo = "" + (System.currentTimeMillis() - 21600000L);
                queryFilter += "isCompleted == false && isAborted == false && startTime > " + sixHoursAgo;
            } else if (!theVerb.equals("filter")) {
                resp.setStatus(404);
                return;
            }
            if (theHost.equals("unsigned")) {
                if (!queryFilter.isEmpty()) queryFilter += " && ";
                queryFilter += "hashedMatchHostPK == null";
            } else if (!theHost.equals("all")) {
                if (!queryFilter.isEmpty()) queryFilter += " && ";
                queryFilter += "hashedMatchHostPK == '" + theHost + "'";
            }
            if (!queryFilter.isEmpty()) {
                query.setFilter(queryFilter);
            }
            if (theDomain.equals("recent")) {            
                query.setOrdering("startTime desc");
                query.setRange(0, 50);
            } else if (theDomain.equals("recent1000")) {
            	query.setOrdering("startTime desc");
            	query.setRange(0, 1000);
            } else {
                resp.setStatus(404);
                return;
            }
            if (!theSplitParams.isEmpty()) {
                Cursor cursor = Cursor.fromWebSafeString(theSplitParams.pop());
                Map<String, Object> extensionMap = new HashMap<String, Object>();
                extensionMap.put(JDOCursorHelper.CURSOR_EXTENSION, cursor);
                query.setExtensions(extensionMap);
            }
            
            String newCursor = "";
            JSONArray theArray = new JSONArray();
            try {
                List<CondensedMatch> results = (List<CondensedMatch>) query.execute();
                if (!results.isEmpty()) {
                    for (CondensedMatch e : results) {
                        if (theVerb.equals("filterActiveSet")) {
                            theArray.put(e.getMatchURL());
                        } else {
                            theArray.put(e.getMatchJSON());
                        }
                    }
                    Cursor cursor = JDOCursorHelper.getCursor(results);
                    newCursor = cursor.toWebSafeString();                    
                } else {
                    // ... no results ...
                }
                
                if (results.size() < 50) {
                	newCursor = "";
                }
            } finally {
                query.closeAll();
            }
            
            try {
                theResponse = new JSONObject();
                theResponse.put("queryMatches", theArray);
                theResponse.put("queryCursor", newCursor);
            } catch (JSONException je) {
                ;
            }
        }
        if (theResponse != null) {
        	resp.setContentType("application/json");
            resp.getWriter().println(theResponse.toString());
        } else {
            resp.setStatus(404);
        }
    }
    
    public static JSONObject runLogQuery(HttpServletResponse resp, String theRPC) throws IOException {        
        Deque<String> theSplitParams = new ArrayDeque<String>(Arrays.asList(theRPC.split(",")));
        theSplitParams.pop();
        String theMatchURL = theSplitParams.pop();
        
        Query query = Persistence.getPersistenceManager().newQuery(MatchLog.class);
        String queryFilter = "matchURL == '" + theMatchURL + "'";
        query.setFilter(queryFilter);
                
        JSONArray theArray = new JSONArray();
        try {
            @SuppressWarnings("unchecked")
			List<MatchLog> results = (List<MatchLog>) query.execute();
            if (!results.isEmpty()) {
                for (MatchLog e : results) {
                	theArray.put(e.playerName);
                }
            } else {
                // ... no results ...
            }
        } finally {
            query.closeAll();
        }
        
        JSONObject theResponse = null;
        try {
            theResponse = new JSONObject();
            theResponse.put("playerLogs", theArray);
        } catch (JSONException je) {
            ;
        }
        return theResponse;
    }
    
    public static JSONObject runExportQuery(HttpServletResponse resp, String theRPC) throws IOException {        
        Query query = Persistence.getPersistenceManager().newQuery(CondensedMatch.class);
        query.setOrdering("startTime desc");
        query.setRange(0, 1000);
        
        JSONArray theArray = new JSONArray();
        try {
            @SuppressWarnings("unchecked")
			List<CondensedMatch> results = (List<CondensedMatch>) query.execute();
            if (!results.isEmpty()) {
                for (CondensedMatch e : results) {
                	theArray.put(e.getMatchJSON());
                }
            }
        } finally {
            query.closeAll();
        }
        
        try {
            JSONObject theResponse = new JSONObject();
            theResponse.put("matches", theArray);
            return theResponse;
        } catch (JSONException je) {
            return null;
        }
    }
}