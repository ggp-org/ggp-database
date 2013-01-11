package ggp.database.cron;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.appengine.api.blobstore.BlobInfo;
import com.google.appengine.api.blobstore.BlobInfoFactory;
import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreServiceFactory;

public class CleanupOldBlobs {
	public static void cleanupOldBlobs() {	    
	    Iterator<BlobInfo> iterator = new BlobInfoFactory().queryBlobInfos();
	    Map<String,BlobInfo> latestBlobs = new HashMap<String,BlobInfo>();
	    Set<BlobInfo> allTheBlobs = new HashSet<BlobInfo>();
	    while(iterator.hasNext()){
	    	BlobInfo blob = iterator.next();
	    	String filename = blob.getFilename();
	    	if (!latestBlobs.containsKey(filename)) {
	    		latestBlobs.put(filename, blob);
	    	} else if (blob.getCreation().after(latestBlobs.get(filename).getCreation())) {
	    		latestBlobs.put(filename, blob);
	    	}
	    	allTheBlobs.add(blob);	    
	    }
	    Set<BlobKey> orphanedBlobKeys = new HashSet<BlobKey>();
	    for (BlobInfo blob : allTheBlobs) {
	    	if (!latestBlobs.values().contains(blob)) {
	    		orphanedBlobKeys.add(blob.getBlobKey());
	    	}
	    }
	    BlobstoreServiceFactory.getBlobstoreService().delete(orphanedBlobKeys.toArray(new BlobKey[]{}));
	}
}