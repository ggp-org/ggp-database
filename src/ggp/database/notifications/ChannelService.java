package ggp.database.notifications;

import ggp.database.Persistence;
import ggp.database.matches.CondensedMatch;

import java.io.IOException;
import java.util.Random;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.channel.ChannelServiceFactory;

public class ChannelService {
    // Channel token handling: we need to be able to create channel tokens/IDs,
    // and register those IDs with particular matches so that we can push updates
    // to the appropriate channels whenever a particular match is updated.
    // Note that creation of channel token/IDs is done centrally: you request a
    // channel token/ID, then you register it with particular matches. This lets
    // you get updates about multiple matches in the same browser session.

    public static void writeChannelToken(HttpServletResponse resp) throws IOException {        
        String theClientID = getRandomString(32);
        String theToken = ChannelServiceFactory.getChannelService().createChannel(theClientID);
        resp.getWriter().println("theChannelID = \"" + theClientID + "\";\n");
        resp.getWriter().println("theChannelToken = \"" + theToken + "\";\n");
    }    

    public static void registerChannelForMatch(HttpServletResponse resp, String theKey, String theClientID) throws IOException {
        PersistenceManager pm = Persistence.getPersistenceManager();
        try {
            CondensedMatch theMatch = pm.detachCopy(pm.getObjectById(CondensedMatch.class, theKey));
            if (theMatch.addClientID(theClientID)) {
                pm.makePersistent(theMatch);
                resp.getWriter().write("Registered for [" + theKey + "] as [" + theClientID + "].");
            } else {
                resp.getWriter().write("Match [" + theKey + "] already completed; no need to register.");
            }
        } catch(JDOObjectNotFoundException e) {
            resp.getWriter().write("Could not find match [" + theKey + "].");
        } finally {
            pm.close();
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