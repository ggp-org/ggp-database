package ggp.database.mapreduce;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.mapreduce.AppEngineMapper;

import org.apache.hadoop.io.NullWritable;

public class PurgeMapper extends AppEngineMapper<Key, Entity, NullWritable, NullWritable> {
  // Map over the datastore, identifying and purging entries which satisfy some criteria.
  public void map(Key key, Entity value, Context context) {
    try {
        if(shouldPurge(value)) {
            DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
            datastore.delete(key);
            context.getCounter("Overall", "Purged").increment(1);
        } else {
            context.getCounter("Overall", "Ignored").increment(1);
        }
    } catch (Exception e) {
        context.getCounter("Overall", "Unreadable").increment(1);
    }
  }
  
  public static boolean shouldPurge(Entity value) {
      return value.hasProperty("hashedMatchHostPK") && value.getProperty("hashedMatchHostPK") != null && value.getProperty("hashedMatchHostPK").equals("0ca7065b86d7646166d86233f9e23ac47d8320d4");
  }
}