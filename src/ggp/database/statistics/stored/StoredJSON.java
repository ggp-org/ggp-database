package ggp.database.statistics.stored;

import ggp.database.Persistence;

import javax.jdo.PersistenceManager;
import javax.jdo.annotations.*;

import com.google.appengine.api.datastore.Text;
import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

@PersistenceCapable
@Inheritance(strategy=InheritanceStrategy.SUBCLASS_TABLE)
public abstract class StoredJSON {
    @SuppressWarnings("unused")
    @PrimaryKey @Persistent private String thePrimaryKey;
    @Persistent private Text theData;
    
    protected StoredJSON(String theKey) {
        thePrimaryKey = theKey;
        theData = new Text(new JSONObject().toString());
    }
    
    public JSONObject getJSON() {
        try {
            return new JSONObject(theData.getValue());
        } catch (JSONException e) {
            return null;
        }        
    }
    
    public void setJSON(JSONObject theJSON) {
        theData = new Text(theJSON.toString());
        save();
    }
    
    private void save() {
        PersistenceManager pm = Persistence.getPersistenceManager();
        pm.makePersistent(this);
        pm.close();        
    }
}