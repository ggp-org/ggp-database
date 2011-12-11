package ggp.database.mapreduce;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.mapreduce.AppEngineMapper;

import org.apache.hadoop.io.NullWritable;

import java.util.List;

public class FieldStatsMapper extends AppEngineMapper<Key, Entity, NullWritable, NullWritable> {  
  // Quick observation map over the datastore, to collect numbers
  // on how often various match descriptor fields are being used.
  @SuppressWarnings("unchecked")
  public void map(Key key, Entity value, Context context) {
    if (!value.hasProperty("gameName")) {
        context.getCounter("gameName", "None").increment(1);
    } else if (value.getProperty("gameName") == null ) {
        context.getCounter("gameName", "Null").increment(1);
    } else if (value.getProperty("gameName").toString().isEmpty()) {
        context.getCounter("gameName", "Empty").increment(1);
    } else {
        context.getCounter("gameName", "Real").increment(1);
    }

    if (!value.hasProperty("gameRoleNames")) {
        context.getCounter("gameRoleNames", "None").increment(1);
    } else if (value.getProperty("gameRoleNames") == null ) {
        context.getCounter("gameRoleNames", "Null").increment(1);
    } else if (((List<String>)value.getProperty("gameRoleNames")).isEmpty()) {
        context.getCounter("gameRoleNames", "Empty").increment(1);
    } else {
        context.getCounter("gameRoleNames", "Real").increment(1);
    }    
  }
}