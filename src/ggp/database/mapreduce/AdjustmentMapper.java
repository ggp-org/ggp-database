package ggp.database.mapreduce;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.mapreduce.AppEngineMapper;

import org.apache.hadoop.io.NullWritable;

public class AdjustmentMapper extends AppEngineMapper<Key, Entity, NullWritable, NullWritable> {
  // Map over the datastore, identifying and adjusting entries which satisfy some criteria.
  public void map(Key key, Entity value, Context context) {
      try {
          if(shouldAdjust(value)) {
              try {
                  doAdjustment(value);
                  DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
                  datastore.put(value);
                  context.getCounter("Overall", "Adjusted").increment(1);
              } catch (Exception e) {
                  context.getCounter("Overall", "Failure").increment(1);
              }
          } else {
              context.getCounter("Overall", "Ignored").increment(1);
          }          
      } catch (Exception e) {
          context.getCounter("Overall", "Unreadable").increment(1);
      }
  }
  
  public static void doAdjustment(Entity value) {
      // Perform adjustment!
  }
  
  public static boolean shouldAdjust(Entity value) {
      // Should the value be adjusted?
      return false;
  }
}