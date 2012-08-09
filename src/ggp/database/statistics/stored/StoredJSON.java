package ggp.database.statistics.stored;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import ggp.database.Persistence;
import ggp.database.statistics.StringCompressor;

import javax.jdo.PersistenceManager;
import javax.jdo.annotations.*;

import com.google.appengine.api.datastore.Text;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;

@PersistenceCapable
@Inheritance(strategy=InheritanceStrategy.SUBCLASS_TABLE)
public abstract class StoredJSON {
    @PrimaryKey @Persistent private String thePrimaryKey;
    @Persistent private Text theData;
    
    protected StoredJSON(String theKey) {
        thePrimaryKey = theKey;
        theData = new Text(StringCompressor.compress(new JSONObject().toString()));
    }
    
    public String getKey() {
        return thePrimaryKey;
    }
    
    public JSONObject getJSON() {
        try {
            return new JSONObject(StringCompressor.decompress(theData.getValue()));
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }        
    }
    
    public void setJSON(JSONObject theJSON) {
        theData = new Text(StringCompressor.compress(truncateDoublesForJSON(theJSON)));
        save();
    }
    
    public void save() {
        PersistenceManager pm = Persistence.getPersistenceManager();
        pm.makePersistent(this);
        pm.close();        
    }
    
    // What is this? Well... we store a compressed version of the
    // string representation of the JSON. The string representation
    // of the JSON is dominated by variable names (which compress
    // well, since they're frequently repeated) and floating point
    // values (with many decimal places, which don't compress well).
    // Since that degree of precision really isn't necessary when
    // computing average scores and such, this function is used to
    // truncate the doubles that appear in the JSON to two decimal
    // places, so they take less room to store. This is really a
    // messy hack to avoid storing over 1MB of data in a single
    // datastore entry.
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