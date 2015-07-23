package ggp.database.statistics.stored;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.ggp.galaxy.shared.persistence.Persistence;

import external.JSON.JSONArray;
import external.JSON.JSONException;
import external.JSON.JSONObject;
import external.JSON.JSONString;
import ggp.database.statistics.StringCompressor;

import javax.jdo.PersistenceManager;
import javax.jdo.annotations.*;

import com.google.appengine.api.datastore.Text;
import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsOutputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;

@PersistenceCapable
@Inheritance(strategy=InheritanceStrategy.SUBCLASS_TABLE)
public abstract class StoredJSON {
    @PrimaryKey @Persistent private String thePrimaryKey;
    @Persistent private Text theData;
    
    protected StoredJSON(String theKey) {
        thePrimaryKey = theKey;
        theData = null;
    }
    
    public String getKey() {
        return thePrimaryKey;
    }

    public JSONObject getJSON() {
    	if (theData != null) {
	        try {
	            return new JSONObject(StringCompressor.decompress(theData.getValue()));
	        } catch (JSONException e) {
	            throw new RuntimeException(e);
	        }
    	} else {
    	    try {
        		GcsService gcsService = GcsServiceFactory.createGcsService();
        		GcsFilename filename = new GcsFilename("ggp-database-storedjson", thePrimaryKey);
        	    GcsInputChannel readChannel = gcsService.openReadChannel(filename, 0);
        	    String theBlobData = new BufferedReader(Channels.newReader(readChannel, "UTF8")).readLine();
        	    readChannel.close();
        	    return new JSONObject(StringCompressor.decompress(theBlobData));
    	    } catch (Exception e) {
    	    	throw new RuntimeException(e);
    	    }
    	}
    }
    
    public void setJSON(JSONObject theJSON) {
    	String theStoredData = StringCompressor.compress(truncateDoublesForJSON(theJSON));

    	if (theStoredData.length() < 100000) {
    		theData = new Text(theStoredData);
    	} else {
    		theData = null;

    		try {
	    	    GcsService gcsService = GcsServiceFactory.createGcsService();
	    	    GcsFilename filename = new GcsFilename("ggp-database-storedjson", thePrimaryKey);
	    	    GcsFileOptions options = new GcsFileOptions.Builder()
	    	        .mimeType("text/plain")
	    	        .acl("public-read")
	    	        .build();
	    	    GcsOutputChannel writeChannel = gcsService.createOrReplace(filename, options);
	    	    PrintWriter writer = new PrintWriter(Channels.newWriter(writeChannel, "UTF8"));
	    	    writer.println(theStoredData);
	    	    writer.flush();
	    	    writer.close();
	    	    writeChannel.close();
	        } catch (Exception e) {
	    		throw new RuntimeException(e);
	    	}
    	}
    	
    	save();
    }

    public void save() {
        PersistenceManager pm = Persistence.getPersistenceManager();
        pm.makePersistent(this);
        pm.close();        
    }
    
    // This method truncates doubles stored in JSON objects. This results in
    // an average 25% savings in storage, because the JSON objects are often
    // dominated by variable names (which compress well, since they're often
    // repeated) and doubles (with many decimal places, which don't compress
    // well since they're hard to predict). Since a high degree of precision
    // isn't necessary for the statistics we're storing, this function can be
    // used to truncate the doubles to two decimal places.
    static String truncateDoublesForJSON(Object x) {
        try {
            if (x instanceof JSONObject) {
                JSONObject theObject = (JSONObject)x;
                
                // Sort the keys
                TreeSet<String> t = new TreeSet<String>();
                Iterator<?> i = theObject.keys();
                while (i.hasNext()) t.add(i.next().toString());
                Iterator<String> keys = t.iterator();
                
                StringBuffer sb = new StringBuffer("{");    
                while (keys.hasNext()) {
                    if (sb.length() > 1) {
                        sb.append(',');
                    }
                    Object o = keys.next();
                    sb.append(JSONObject.quote(o.toString()));
                    sb.append(':');
                    sb.append(truncateDoublesForJSON(theObject.get(o.toString())));
                }
                sb.append('}');
                return sb.toString();
            } else if (x instanceof JSONArray) {
                JSONArray theArray = (JSONArray)x;
                StringBuffer sb = new StringBuffer();
                sb.append("[");
                int len = theArray.length();
                for (int i = 0; i < len; i += 1) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    sb.append(truncateDoublesForJSON(theArray.get(i)));
                }
                sb.append("]");
                return sb.toString();
            } else {
                if (x == null || x.equals(null)) {
                    return "null";
                }
                if (x instanceof JSONString) {
                    Object object;
                    try {
                        object = ((JSONString)x).toJSONString();
                    } catch (Exception e) {
                        throw new JSONException(e);
                    }
                    if (object instanceof String) {
                        return (String)object;
                    }
                    throw new JSONException("Bad value from toJSONString: " + object);
                }
                if (x instanceof Number) {
                	if (x instanceof Double) {
                		x = Math.floor((Double)x*100)/100.0;
                	}
                    return JSONObject.numberToString((Number)x);
                }
                if (x instanceof Boolean || x instanceof JSONObject ||
                        x instanceof JSONArray) {
                    return x.toString();
                }
                if (x instanceof Map) {
                    return truncateDoublesForJSON(new JSONObject((Map<?,?>)x)).toString();
                }
                if (x instanceof Collection) {
                    return truncateDoublesForJSON(new JSONArray((Collection<?>)x)).toString();
                }
                if (x.getClass().isArray()) {
                    return truncateDoublesForJSON(new JSONArray(x)).toString();
                }
                return JSONObject.quote(x.toString());
            }
        } catch (Exception e) {
            return null;
        }            
    }    
}