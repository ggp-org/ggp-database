package ggp.database.statistics.stored;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.nio.channels.Channels;

import org.ggp.galaxy.shared.persistence.Persistence;

import external.JSON.JSONException;
import external.JSON.JSONObject;
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
    	String theStoredData = StringCompressor.compress(theJSON.toString());

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
}