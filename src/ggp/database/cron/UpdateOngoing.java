package ggp.database.cron;

import static com.google.appengine.api.taskqueue.RetryOptions.Builder.withTaskRetryLimit;
import static com.google.appengine.api.taskqueue.TaskOptions.Builder.withUrl;
import ggp.database.Persistence;
import ggp.database.matches.CondensedMatch;

import java.io.IOException;
import java.util.List;

import javax.jdo.Query;

import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions.Method;

public class UpdateOngoing {
    @SuppressWarnings("unchecked")
    public static void run() throws IOException {
        Query query = Persistence.getPersistenceManager().newQuery(CondensedMatch.class);
        query.setFilter("isCompleted == false");

        try {
            List<CondensedMatch> results = (List<CondensedMatch>) query.execute();
            for (CondensedMatch e : results) {
                QueueFactory.getDefaultQueue().add(withUrl("/tasks/ingest_match").method(Method.GET).param("matchURL", e.getMatchURL()).retryOptions(withTaskRetryLimit(2)));            
            }
        } finally {
            query.closeAll();
        }
    }
}