package ggp.database.statistics.statistic;

import java.util.HashMap;
import java.util.Map;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.repackaged.org.json.JSONArray;
import com.google.appengine.repackaged.org.json.JSONException;

public abstract class PerRoleStatistic<T extends Statistic> extends Statistic {
    private static final String rolePrefix = "role_";
    
    Map<Integer, T> cachedRoleStats = new HashMap<Integer, T>();
    public T getPerRoleStatistic(int nRole) {
        try {
            if (cachedRoleStats.containsKey(nRole)) {
                return cachedRoleStats.get(nRole);
            } else {
                T aStat = getInitialStatistic();
                if (getState().has(rolePrefix + nRole)) {
                    aStat.setState(getState().getJSONObject(rolePrefix + nRole));
                } else {                
                    getState().put(rolePrefix + nRole, aStat.getState());
                }
                cachedRoleStats.put(nRole, aStat);
                return aStat;
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    protected final Object getFinalForm() throws JSONException {
        JSONArray theFinalForm = new JSONArray();
        int nRole = 0;
        while (getState().has(rolePrefix + nRole)) {
            theFinalForm.put(getPerRoleStatistic(nRole).getFinalForm());
            nRole++;
        }
        return theFinalForm;
    }
    
    public int getKnownRoleCount() {
        int nRole = 0;
        while (getState().has(rolePrefix + nRole))
            nRole++;
        return nRole;
    }
    
    public abstract void updateWithMatch(Entity newMatch);
    protected abstract T getInitialStatistic();    
}