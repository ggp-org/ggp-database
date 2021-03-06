package ggp.database;

import static com.google.appengine.api.taskqueue.RetryOptions.Builder.withTaskRetryLimit;
import static com.google.appengine.api.taskqueue.TaskOptions.Builder.withUrl;
import external.JSON.JSONException;
import external.JSON.JSONObject;
import ggp.database.cron.UpdateOngoing;
import ggp.database.logs.MatchLog;
import ggp.database.matches.CondensedMatch;
import ggp.database.notifications.ChannelService;
import ggp.database.notifications.UpdateRegistry;
import ggp.database.queries.MatchQuery;
import ggp.database.reports.DailyReport;
import ggp.database.statistics.MatchStatistics;
import ggp.database.statistics.StatisticsComputation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.logging.Logger;

import javax.servlet.http.*;

import com.google.appengine.api.backends.BackendServiceFactory;
import com.google.appengine.api.capabilities.CapabilitiesService;
import com.google.appengine.api.capabilities.CapabilitiesServiceFactory;
import com.google.appengine.api.capabilities.Capability;
import com.google.appengine.api.capabilities.CapabilityStatus;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.ReadPolicy;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions.Method;

import org.ggp.base.util.loader.RemoteResourceLoader;
import org.ggp.galaxy.shared.persistence.Persistence;

@SuppressWarnings("serial")
public class GGP_DatabaseServlet extends HttpServlet {
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        resp.setHeader("Access-Control-Allow-Headers", "*");
        resp.setHeader("Access-Control-Allow-Age", "86400");
        resp.setStatus(200);
        
        String reqURI = req.getRequestURI();
        if (reqURI.equals("/cron/push_subscribe") || reqURI.equals("/push_subscribe")) {
            ServerState ss = ServerState.loadState();
            ss.rotateValidationToken();
            ss.save();
            String theCallbackURL = URLEncoder.encode("http://database.ggp.org/ingestion/", "UTF-8");
            String theFeedURL = URLEncoder.encode("http://matches.ggp.org/matches/feeds/updatedFeed.atom", "UTF-8");
            int nAttempt = 0;
            while (true) {
	            try {
	            	RemoteResourceLoader.postRawWithTimeout("http://pubsubhubbub.appspot.com/", "hub.callback=" + theCallbackURL + "&hub.mode=subscribe&hub.topic=" + theFeedURL + "&hub.verify=sync&hub.verify_token=" + ss.getValidationToken(), 5000);
	                resp.getWriter().println("PuSH subscription sent.");            
	                return;
	            } catch (IOException ie) {
	            	if (nAttempt > 5) {
	            		Logger.getAnonymousLogger().severe("Caught exception while subscribing to PuSH: " + ie);
	            	} else if (nAttempt > 8) {
	            		throw new RuntimeException(ie);
	            	}
	            }
	            nAttempt++;
            }
        } else if (req.getRequestURI().equals("/cron/update_stats") || req.getRequestURI().equals("/update_stats")) {
            QueueFactory.getDefaultQueue().add(withUrl("/tasks/update_stats").method(Method.GET).header("Host", BackendServiceFactory.getBackendService().getBackendAddress("stats")).retryOptions(withTaskRetryLimit(0)));
            return;
        } else if (reqURI.equals("/cron/update_all_ongoing") || reqURI.equals("/update_ongoing")) {
            UpdateOngoing.updateAllOngoing();
            return;
        } else if (reqURI.equals("/cron/update_recent_ongoing")) {
            UpdateOngoing.updateRecentOngoing();
            return;
        } else if (reqURI.equals("/cron/generate_player_reports")) {
            for (DailyReport theReport : Persistence.loadAll(DailyReport.class)) {
            	theReport.generateReport();
            }
        	return;
        } else if (req.getRequestURI().equals("/tasks/update_stats")) {
            if (isDatastoreWriteable()) {
                StatisticsComputation.computeBatchStatistics();
            }
            return;
        }     
        
        if (reqURI.equals("/ingestion/")) {
            // Handle the PuSH subscriber confirmation
            boolean isValid = true;
            ServerState ss = ServerState.loadState();
            isValid &= req.getParameter("hub.topic").equals("http://matches.ggp.org/matches/feeds/updatedFeed.atom");
            isValid &= req.getParameter("hub.verify_token").equals(ss.getValidationToken());
            if (isValid) {
                resp.getWriter().print(req.getParameter("hub.challenge"));
                resp.getWriter().close();
            } else {
                resp.setStatus(404);
                resp.getWriter().close();
            }
            return;
        } else if (reqURI.equals("/ingest_match")) {
        	QueueFactory.getDefaultQueue().add(withUrl("/tasks/ingest_match").method(Method.GET).param("matchURL", req.getParameter("matchURL")).retryOptions(withTaskRetryLimit(INGESTION_RETRIES)));
        	return;
        } else if (reqURI.equals("/tasks/ingest_match")) {
            // Actually ingest a match, in the task queue.
        	String theMatchURL = req.getParameter("matchURL");
        	CondensedMatch theMatch = null;
        	try {
	            JSONObject theMatchJSON = RemoteResourceLoader.loadJSON(theMatchURL, 14);
	            theMatch = CondensedMatch.storeCondensedMatchJSON(theMatchURL, theMatchJSON);
        	} catch (Exception e) {
        		// For the first few exceptions, silently issue errors to task queue to trigger retries.
        		// After a few retries, start surfacing the exceptions, since they're clearly not transient.
            	// This reduces the amount of noise in the error logs caused by transient server errors.
        		resp.setStatus(503);
        		Logger.getAnonymousLogger().severe(e.getMessage());
        		int nRetryAttempt = Integer.parseInt(req.getHeader("X-AppEngine-TaskRetryCount"));
            	if (nRetryAttempt > INGESTION_RETRIES - 3) {
            		throw new RuntimeException(e);
            	}
            	return;
        	}
            if (theMatch.isCompleted || (theMatch.isAborted != null && theMatch.isAborted)) {
                //QueueFactory.getQueue("stats").add(withUrl("/tasks/live_update_stats").param("matchURL", theMatchURL).method(Method.GET).retryOptions(withTaskRetryLimit(0)));
                for (String aPlayer : theMatch.playerNamesFromHost) {
                	// TODO(schreib): Figure out a way to not restrict this based on host; instead have a way to retrieve player information regardless of host.
                	if (aPlayer != null && !aPlayer.isEmpty() && !aPlayer.toLowerCase().equals("random") && theMatch.hashedMatchHostPK != null && theMatch.hashedMatchHostPK.equals("90bd08a7df7b8113a45f1e537c1853c3974006b2")) {
                		QueueFactory.getQueue("logfetch").add(withUrl("/tasks/fetch_log").param("matchURL", theMatchURL).param("playerName", aPlayer).param("matchID", theMatch.matchId).method(Method.GET).retryOptions(withTaskRetryLimit(FETCH_LOG_RETRIES)));
                	}
                }
            }        	
            return;
        } else if (reqURI.equals("/tasks/live_update_stats")) {
            String theMatchURL = req.getParameter("matchURL");
            DatastoreService datastore = DatastoreServiceFactory.getDatastoreService(DatastoreServiceConfig.Builder.withReadPolicy(new ReadPolicy(ReadPolicy.Consistency.EVENTUAL)));
            try {
                StatisticsComputation.incrementallyAddMatch(datastore.get(KeyFactory.createKey("CondensedMatch", theMatchURL)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return;
        } else if (reqURI.equals("/tasks/fetch_log")) {
            String theMatchID = req.getParameter("matchID");
            String theMatchURL = req.getParameter("matchURL");            
            String thePlayerName = req.getParameter("playerName");
            
            JSONObject theMatchJSON = null;
        	try {
	            theMatchJSON = RemoteResourceLoader.loadJSON("http://tiltyard.ggp.org/data/players/" + thePlayerName);
        	} catch (Exception e) {        		
        		// For the first few exceptions, silently issue errors to task queue to trigger retries.
        		// After a few retries, start surfacing the exceptions, since they're clearly not transient.
            	// This reduces the amount of noise in the error logs caused by transient server errors.
        		resp.setStatus(503);
        		int nRetryAttempt = Integer.parseInt(req.getHeader("X-AppEngine-TaskRetryCount"));
            	if (nRetryAttempt > FETCH_LOG_RETRIES - 3) {            		
            		throw new RuntimeException(e);
            	}
            	return;
        	}
        	
        	String thePlayerAddress = null;
        	try {
        		if (!theMatchJSON.has("exponentURL") || theMatchJSON.getString("exponentURL").isEmpty()) {
        			return;
        		}
        		thePlayerAddress = theMatchJSON.getString("exponentURL");        		
        	} catch (JSONException j) {
        		throw new RuntimeException(j);
        	}
        	
            JSONObject theData = null;
        	try {
        		theData = RemoteResourceLoader.loadJSON("http://" + thePlayerAddress + "/" + theMatchID, 7);
        	} catch (Exception e) {        		
        		// For the first few exceptions, silently issue errors to task queue to trigger retries.
        		// After a few retries, start surfacing the exceptions, since they're clearly not transient.
            	// This reduces the amount of noise in the error logs caused by transient server errors.
        		//resp.setStatus(503);
        		resp.setStatus(200);
        		/* TODO(schreib): Figure out the right way to handle players whose log summarizers
        		 * are not responsive to the database log fetching requests. Don't just spam the error
        		 * log, since that's clearly not working.
        		int nRetryAttempt = Integer.parseInt(req.getHeader("X-AppEngine-TaskRetryCount"));
            	if (nRetryAttempt > FETCH_LOG_RETRIES - 3) {
            		throw new RuntimeException(e);
            	}
            	*/
        		Logger.getAnonymousLogger().severe("Failure when fetching logs for player: " + thePlayerName + " due to " + e.toString() + " caused by " + e.getCause());
            	return;
        	}
        	
            if (theData != null && theData.length() > 0) {
                try {
                    new MatchLog(thePlayerName, theMatchURL, theData);
                } catch (JSONException e) {
                    throw new RuntimeException(e);
                }
            }
        	
            return;            
        }

        // Handle requests for browser channel subscriptions.
        if (reqURI.startsWith("/subscribe/")) {
            String theSub = reqURI.replace("/subscribe/", "");
            if (theSub.equals("channel.js")) {
                // If they're requesting a channel token, we can handle
                // that immediately without needing further parsing.                
                ChannelService.writeChannelToken(resp);
                return;
            } else if (theSub.startsWith("match/")) {
                theSub = theSub.substring("match/".length());
                // Parse out the match URL and the channel token, and subscribe
                // that channel token to that match URL.
                if (theSub.contains("/clientId=")) {
                    String theID = theSub.substring(theSub.indexOf("/clientId=")+("/clientId=".length()));
                    String theKey = theSub.substring(0, theSub.indexOf("/clientId="))+"/";
                    ChannelService.registerChannelForMatch(resp, theKey, theID);
                    return;
                }
            } else if (theSub.startsWith("query/")) {
                // Parse out the query string and the channel token, and subscribe
                // that channel token to that match URL.
                if (theSub.contains("/clientId=")) {
                    String theID = theSub.substring(theSub.indexOf("/clientId=")+("/clientId=".length()));
                    String theKey = theSub.substring(0, theSub.indexOf("/clientId="));
                    if (UpdateRegistry.verifyKey(theKey)) {
                        UpdateRegistry.registerClient(theKey, theID);
                        resp.getWriter().println("Successfully subscribed to: " + theKey);
                        return;
                    }
                }
            }
            resp.setStatus(404);
            return;
        }

        if (reqURI.startsWith("/query/")) {
            MatchQuery.respondToQuery(resp, reqURI.replaceFirst("/query/", ""));
            return;
        }
        if (reqURI.startsWith("/statistics/")) {
            MatchStatistics.respondWithStats(resp, reqURI.replaceFirst("/statistics/", ""));
            return;
        }
        if (reqURI.startsWith("/logs/")) {
            MatchLog.respondWithLog(resp, reqURI.replaceFirst("/logs/", ""));
            return;
        }

        resp.setStatus(404);
    }
    
    private static final int FETCH_LOG_RETRIES = 5;
    private static final int INGESTION_RETRIES = 25;
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        resp.setHeader("Access-Control-Allow-Headers", "*");
        resp.setHeader("Access-Control-Allow-Age", "86400");
        
        if (req.getRequestURI().equals("/ingestion/")) {
            BufferedReader br = new BufferedReader(new InputStreamReader(req.getInputStream()));
            int contentLength = Integer.parseInt(req.getHeader("Content-Length").trim());
            StringBuilder theInput = new StringBuilder();
            for (int i = 0; i < contentLength; i++) {
                theInput.append((char)br.read());
            }
            String in = theInput.toString().trim();

            String theLink = in.replace("http://matches.ggp.org/matches/feeds/updatedFeed.atom", "");
            theLink = theLink.replace("http://matches.ggp.org/matches/feeds/completedFeed.atom", "");
            theLink = theLink.substring(theLink.indexOf("<link href=\"http://matches.ggp.org/matches/"));
            theLink = theLink.substring("<link href=\"http://matches.ggp.org/matches/".length(), theLink.indexOf("\"/>"));
            theLink = "http://matches.ggp.org/matches/" + theLink;

            QueueFactory.getDefaultQueue().add(withUrl("/tasks/ingest_match").method(Method.GET).param("matchURL", theLink).retryOptions(withTaskRetryLimit(INGESTION_RETRIES)));

            resp.setStatus(200);
            resp.getWriter().close();
        }
    }

    public static boolean isDatastoreWriteable() {
        CapabilitiesService service = CapabilitiesServiceFactory.getCapabilitiesService();
        CapabilityStatus status = service.getStatus(Capability.DATASTORE_WRITE).getStatus();
        return (status != CapabilityStatus.DISABLED);
    }

    public void doOptions(HttpServletRequest req, HttpServletResponse resp) throws IOException {  
        resp.setHeader("Access-Control-Allow-Origin", "*");
        resp.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        resp.setHeader("Access-Control-Allow-Headers", "*");
        resp.setHeader("Access-Control-Allow-Age", "86400");
    }    
}