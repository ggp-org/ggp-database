package ggp.database.queries;

import org.ggp.shared.persistence.Persistence;
import ggp.database.matches.CondensedMatch;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jdo.Query;
import javax.servlet.http.HttpServletResponse;

import org.datanucleus.store.appengine.query.JDOCursorHelper;

import com.google.appengine.api.datastore.Cursor;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class MatchQuery {
    @SuppressWarnings("unchecked")
    public static void respondToQuery(HttpServletResponse resp, String theRPC) throws IOException {
        JSONObject theResponse = null;
        if (theRPC.startsWith("filter")) {
            Deque<String> theSplitParams = new ArrayDeque<String>(Arrays.asList(theRPC.split(",")));
            String theVerb, theDomain, theHost;
            try {
                theVerb = theSplitParams.pop();
                theDomain = theSplitParams.pop();
                theHost = theSplitParams.pop();
            } catch (ArrayIndexOutOfBoundsException e) {
                resp.setStatus(404);
                return;
            }
            
            if (theVerb.length() == 0 || theDomain.length() == 0 || theHost.length() == 0) {
                resp.setStatus(404);
                return;
            }
            Query query = Persistence.getPersistenceManager().newQuery(CondensedMatch.class);
            String queryFilter = "";
            if (theVerb.equals("filterPlayer")) {
                String thePlayer = theSplitParams.pop();
                if (thePlayer.length() == 0) {
                    resp.setStatus(404);
                    return;
                }
                queryFilter += "playerNamesFromHost == '" + thePlayer + "'";
            } else if (theVerb.equals("filterGame")) {
                String theGame = theSplitParams.pop();
                if (theGame.length() == 0) {
                    resp.setStatus(404);
                    return;
                }
                queryFilter += "gameMetaURL == '" + theGame + "'";
            } else if (theVerb.equals("filterActiveSet")) {
                String sixHoursAgo = "" + (System.currentTimeMillis() - 21600000L);
                queryFilter += "isCompleted == false && startTime > " + sixHoursAgo;
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
                        Cursor cursor = JDOCursorHelper.getCursor(results);
                        newCursor = cursor.toWebSafeString();
                    }
                } else {
                    // ... no results ...
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
            resp.getWriter().println(theResponse.toString());
        } else {
            resp.setStatus(404);
        }
    }
}