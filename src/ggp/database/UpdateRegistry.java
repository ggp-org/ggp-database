package ggp.database;

import ggp.database.matches.CondensedMatch;
import ggp.database.Persistence;

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

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;
import javax.jdo.annotations.*;
import javax.servlet.http.HttpServletResponse;

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
public class UpdateRegistry {
    @PrimaryKey @Persistent private String registryKey;
    @Persistent private List<String> theClientIDs;
    
    private static final int MAX_SUBSCRIPTIONS = 100;
    
    public UpdateRegistry(String theKey) {
        registryKey = theKey;
        theClientIDs = new ArrayList<String>();
    }
    
    private boolean addClientID(String clientID) {
        theClientIDs.add(clientID);
        if (theClientIDs.size() > MAX_SUBSCRIPTIONS) {
            theClientIDs.remove(0);
        }
        return true;
    }

    public int numClientIDs() {
        return theClientIDs.size();
    }

    public void pingChannelClients() {
        ChannelService chanserv = ChannelServiceFactory.getChannelService();
        if (theClientIDs != null) {
            for(String clientID : theClientIDs) {
                chanserv.sendMessage(new ChannelMessage(clientID, "//database.ggp.org/" + registryKey));
            }
        }
    }    

    /* Static accessor methods */
    public static boolean verifyKey(String theKey) {
        if (theKey.startsWith("query/")) {
            String[] theSplit = theKey.split(",");
            String theVerb = theSplit[0];
            String theDomain = theSplit[1];
            if (!theDomain.equals("recent"))
                return false;            
            if (theVerb.equals("query/filterPlayer")) {
                return (theSplit.length == 4);
            } else if (theVerb.equals("query/filterGame")) {
                return (theSplit.length == 4);
            } else if (theVerb.equals("query/filterActiveSet")) {
                return (theSplit.length == 3);
            } else if (theVerb.equals("query/filter")) {
                return (theSplit.length == 3);
            }
        }
        return false;
    }

    public static void pingClients(String theKey) {
        UpdateRegistry theRegistry = Persistence.loadSpecific(theKey, UpdateRegistry.class);
        if (theRegistry == null) return;
        theRegistry.pingChannelClients();
    }

    public static void registerClient(String theKey, String clientID) {
        UpdateRegistry theRegistry;
        PersistenceManager pm = Persistence.getPersistenceManager();
        try {
            theRegistry = (UpdateRegistry)pm.detachCopy(pm.getObjectById(UpdateRegistry.class, theKey));
        } catch(JDOObjectNotFoundException e) {
            theRegistry = new UpdateRegistry(theKey);
        }
        theRegistry.addClientID(clientID);
        pm.makePersistent(theRegistry);
        pm.close();
    }
}