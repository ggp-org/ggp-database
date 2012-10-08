package ggp.database;

import static com.google.appengine.api.taskqueue.RetryOptions.Builder.withTaskRetryLimit;
import static com.google.appengine.api.taskqueue.TaskOptions.Builder.withUrl;

import ggp.database.cron.UpdateOngoing;
import ggp.database.logs.MatchLog;
import ggp.database.matches.CondensedMatch;
import ggp.database.notifications.ChannelService;
import ggp.database.notifications.UpdateRegistry;
import ggp.database.queries.MatchQuery;
import ggp.database.reports.DailyReport;
import ggp.database.statistics.MatchStatistics;
import ggp.database.statistics.NewStatisticsComputation;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;

import javax.servlet.http.*;

import util.configuration.RemoteResourceLoader;

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
import org.json.JSONException;
import org.json.JSONObject;

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
            RemoteResourceLoader.postRawWithTimeout("http://pubsubhubbub.appspot.com/", "hub.callback=" + theCallbackURL + "&hub.mode=subscribe&hub.topic=" + theFeedURL + "&hub.verify=sync&hub.verify_token=" + ss.getValidationToken(), 5000);
            resp.getWriter().println("PuSH subscription sent.");            
            return;
        } else if (req.getRequestURI().equals("/cron/update_stats") || req.getRequestURI().equals("/update_stats")) {
            QueueFactory.getDefaultQueue().add(withUrl("/tasks/update_stats").method(Method.GET).retryOptions(withTaskRetryLimit(0)));
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
                NewStatisticsComputation.computeBatchStatistics();
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
        } else if (reqURI.equals("/tasks/ingest_match") || reqURI.equals("/ingest_match")) {
            // Actually ingest a match, in the task queue.
            String theMatchURL = req.getParameter("matchURL");
            JSONObject theMatchJSON = RemoteResourceLoader.loadJSON(theMatchURL);
            CondensedMatch theMatch = CondensedMatch.storeCondensedMatchJSON(theMatchURL, theMatchJSON);
            if (theMatch.isCompleted) {
                QueueFactory.getQueue("stats").add(withUrl("/tasks/live_update_stats").param("matchURL", theMatchURL).method(Method.GET).retryOptions(withTaskRetryLimit(0)));
                for (String aPlayer : theMatch.playerNamesFromHost) {
                    if (aPlayer.equals("GreenShell") || aPlayer.equals("CloudKingdom")) {
                        QueueFactory.getQueue("stats").add(withUrl("/tasks/fetch_log").param("matchURL", theMatchURL).param("playerName", aPlayer).param("matchID", theMatch.matchId).method(Method.GET).retryOptions(withTaskRetryLimit(0)));
                    }
                }
            }
            return;
        } else if (reqURI.equals("/tasks/live_update_stats")) {
            String theMatchURL = req.getParameter("matchURL");
            DatastoreService datastore = DatastoreServiceFactory.getDatastoreService(DatastoreServiceConfig.Builder.withReadPolicy(new ReadPolicy(ReadPolicy.Consistency.EVENTUAL)));
            try {
                NewStatisticsComputation.incrementallyAddMatch(datastore.get(KeyFactory.createKey("CondensedMatch", theMatchURL)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return;
        } else if (reqURI.equals("/tasks/fetch_log")) {
            String theMatchID = req.getParameter("matchID");
            String theMatchURL = req.getParameter("matchURL");            
            String thePlayerName = req.getParameter("playerName");
            
            // TODO(schreib): Look this up properly.
            String thePlayerAddress = null;
            if (thePlayerName.equals("GreenShell")) thePlayerAddress = "http://76.102.12.84:9199/";
            if (thePlayerName.equals("CloudKingdom")) thePlayerAddress = "http://76.102.12.84:9198/";
            if (thePlayerAddress == null) return;
            
            JSONObject theData = RemoteResourceLoader.loadJSON(thePlayerAddress + theMatchID);
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

        boolean writeAsBinary = false;
        if (reqURI.endsWith("/")) {
            reqURI += "index.html";
        }
        if (reqURI.endsWith(".html")) {
            resp.setContentType("text/html");
        } else if (reqURI.endsWith(".xml")) {
            resp.setContentType("application/xml");
        } else if (reqURI.endsWith(".xsl")) {
            resp.setContentType("application/xml");
        } else if (reqURI.endsWith(".js")) {
            resp.setContentType("text/javascript");   
        } else if (reqURI.endsWith(".json")) {
            resp.setContentType("text/javascript");
        } else if (reqURI.endsWith(".png")) {
            resp.setContentType("image/png");
            writeAsBinary = true;
        } else if (reqURI.endsWith(".ico")) {
            resp.setContentType("image/png");
            writeAsBinary = true;
        } else {
            resp.setContentType("text/plain");
        }

        try {
            if (writeAsBinary) {
                writeStaticBinaryPage(resp, reqURI.substring(1));
            } else {
                // Temporary limits on caching, for during development.
                resp.setHeader("Cache-Control", "no-cache");
                resp.setHeader("Pragma", "no-cache");
                writeStaticTextPage(resp, reqURI.substring(1));
            }
        } catch(IOException e) {
            resp.setStatus(404);
        }
    }
    
    public void writeStaticTextPage(HttpServletResponse resp, String theURI) throws IOException {
        FileReader fr = new FileReader(theURI);
        BufferedReader br = new BufferedReader(fr);
        StringBuffer response = new StringBuffer();
        
        String line;
        while( (line = br.readLine()) != null ) {
            response.append(line + "\n");
        }

        resp.getWriter().println(response.toString());
    }
    
    public static void writeStaticBinaryPage(HttpServletResponse resp, String theURI) throws IOException {
        InputStream in = new FileInputStream(theURI);
        byte[] buf = new byte[1024];
        while (in.read(buf) > 0) {
            resp.getOutputStream().write(buf);
        }
        in.close();        
    }    

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

            QueueFactory.getDefaultQueue().add(withUrl("/tasks/ingest_match").method(Method.GET).param("matchURL", theLink).retryOptions(withTaskRetryLimit(6)));

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