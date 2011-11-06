package ggp.database.mapreduce;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.repackaged.org.json.JSONObject;
import com.google.appengine.tools.mapreduce.AppEngineMapper;

import org.apache.hadoop.io.NullWritable;

import java.util.List;
import java.util.logging.Logger;

public class FieldStatsMapper extends AppEngineMapper<Key, Entity, NullWritable, NullWritable> {
  private static final Logger log = Logger.getLogger(FieldStatsMapper.class.getName());

  public FieldStatsMapper() {
      ;
  }

  @Override
  public void taskSetup(Context context) {
    log.warning("Doing per-task setup");
  }

  @Override
  public void taskCleanup(Context context) {
    log.warning("Doing per-task cleanup");
  }

  @Override
  public void setup(Context context) {
    log.warning("Doing per-worker setup");
  }

  @Override
  public void cleanup(Context context) {
    log.warning("Doing per-worker cleanup");    
  }
  
  public static void recordWhetherJSONHas(Context context, JSONObject theMatch, String theKey) {
      if (theMatch.has(theKey)) context.getCounter("hasField", theKey).increment(1);
      else context.getCounter("lacksField", theKey).increment(1);
  }

  // Quick observation map over the datastore, to collect numbers
  // on how often various match descriptor fields are being used.
  @SuppressWarnings("unchecked")
  @Override
  public void map(Key key, Entity value, Context context) {
    log.warning("Mapping key: " + key);
    
    try {
        List<String> theNames = (List<String>)value.getProperty("playerNamesFromHost");
        
        if (theNames.size() > 0) {
            context.getCounter("namesList", "hasNames").increment(1);
        } else {
            context.getCounter("namesList", "isEmpty").increment(1);
        }
        
        context.getCounter("Overall", "Readable").increment(1);
    } catch (Exception e) {
        context.getCounter("Overall", "Unreadable").increment(1);
    }
  }
}