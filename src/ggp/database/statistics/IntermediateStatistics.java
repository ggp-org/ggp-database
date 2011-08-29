package ggp.database.statistics;

import ggp.database.Persistence;

import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.annotations.*;

import com.google.appengine.api.datastore.Text;
import com.google.appengine.repackaged.org.json.JSONException;
import com.google.appengine.repackaged.org.json.JSONObject;

@PersistenceCapable
public class IntermediateStatistics {
    @PrimaryKey @Persistent private String thePrimaryKey;
    @Persistent private Text data;
    
    public IntermediateStatistics(String theLabel) {
        thePrimaryKey = theLabel;        
        data = new Text(new JSONObject().toString());
    }
    
    public String getKey() {
        return thePrimaryKey;
    }
    
    public JSONObject getIntermediateData() {
        try {
            return new JSONObject(data.getValue());
        } catch (JSONException e) {
            return null;
        }
    }
    
    public void setIntermediateData(JSONObject newData) {
        data = new Text(newData.toString());
    }

    public void save() {
        PersistenceManager pm = Persistence.getPersistenceManager();
        pm.makePersistent(this);
        pm.close();
    }
    
    /* Static accessor methods */
    public static void saveIntermediateStatistics(String theLabel, JSONObject theJSON) {
        IntermediateStatistics theStats = new IntermediateStatistics(theLabel);
        theStats.setIntermediateData(theJSON);
        theStats.save();
    }
    
    public static JSONObject loadIntermediateStatistics(String theLabel) {
        IntermediateStatistics s = Persistence.loadSpecific(theLabel, IntermediateStatistics.class);
        if (s != null) {
            return s.getIntermediateData();
        }
        return null;
    }
    
    public static Set<IntermediateStatistics> loadAllIntermediateStatistics() {
        return Persistence.loadAll(IntermediateStatistics.class);
    }
}