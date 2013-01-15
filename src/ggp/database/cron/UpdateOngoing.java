package ggp.database.cron;

import static com.google.appengine.api.taskqueue.RetryOptions.Builder.withTaskRetryLimit;
import static com.google.appengine.api.taskqueue.TaskOptions.Builder.withUrl;
import org.ggp.galaxy.shared.persistence.Persistence;
import ggp.database.matches.CondensedMatch;

import java.io.IOException;
import java.util.List;

import javax.jdo.Query;

import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions.Method;

public class UpdateOngoing {
    public static void updateAllOngoing() throws IOException {
        updateOngoingSince(null);
    }

    public static void updateRecentOngoing() throws IOException {
        String sixHoursAgo = "" + (System.currentTimeMillis() - 21600000L);
        updateOngoingSince(sixHoursAgo);
    }

    @SuppressWarnings("unchecked")
    private static void updateOngoingSince(String sinceTime) throws IOException {
        Query query = Persistence.getPersistenceManager().newQuery(CondensedMatch.class);
        if (sinceTime == null) {
            query.setFilter("isCompleted == false");
        } else {
            query.setFilter("isCompleted == false && startTime > " + sinceTime);
        }

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