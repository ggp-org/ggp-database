package ggp.database.cron;

import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.google.appengine.api.blobstore.BlobInfo;
import com.google.appengine.api.blobstore.BlobInfoFactory;
import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreService;
import com.google.appengine.api.blobstore.BlobstoreServiceFactory;

public class CleanupOldBlobs {
	public static void cleanupOldBlobs() {	    
	    List<BlobKey> blobsToDelete = new LinkedList<BlobKey>(); 
	    Iterator<BlobInfo> iterator = new BlobInfoFactory().queryBlobInfos();;
	    while(iterator.hasNext()){	    	
	    	BlobInfo blob = iterator.next();
	    	if (new Date().getTime() - blob.getCreation().getTime() > 129600000) {
	    		blobsToDelete.add(blob.getBlobKey());
	    	}
	    }
	    BlobstoreService blobstoreService = BlobstoreServiceFactory.getBlobstoreService();
	    blobstoreService.delete(blobsToDelete.toArray(new BlobKey[]{}));
	}
}