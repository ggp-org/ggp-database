package ggp.database.statistics.stored;

import ggp.database.Persistence;
import ggp.database.statistics.StringCompressor;

import javax.jdo.PersistenceManager;
import javax.jdo.annotations.*;

import com.google.appengine.api.datastore.Text;
import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

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
        theData = new Text(StringCompressor.compress(theJSON.toString()));
        save();
    }
    
    public void save() {
        PersistenceManager pm = Persistence.getPersistenceManager();
        pm.makePersistent(this);
        pm.close();        
    }
}