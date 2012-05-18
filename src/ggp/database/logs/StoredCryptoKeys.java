package ggp.database.logs;

import ggp.database.Persistence;

import java.io.IOException;

import javax.jdo.PersistenceManager;
import javax.jdo.annotations.*;

import util.crypto.BaseCryptography.EncodedKeyPair;

import com.google.appengine.api.datastore.Text;
import org.json.JSONException;

@PersistenceCapable
public class StoredCryptoKeys {
    @SuppressWarnings("unused")
    @PrimaryKey @Persistent private String thePrimaryKey;
    @Persistent private Text theCryptoKeys;

    private StoredCryptoKeys(String thePrimaryKey) {
        this.thePrimaryKey = thePrimaryKey;
    }

    public EncodedKeyPair getCryptoKeys() {
        try {
            return new EncodedKeyPair(theCryptoKeys.getValue());
        } catch (JSONException e) {
            return null;
        }
    }
    
    public void save() {
        PersistenceManager pm = Persistence.getPersistenceManager();
        pm.makePersistent(this);
        pm.close();        
    }
    
    /* Static accessor methods */
    public static StoredCryptoKeys loadCryptoKeys(String theName) throws IOException {
        return Persistence.loadSpecific(theName, StoredCryptoKeys.class);
   }
}