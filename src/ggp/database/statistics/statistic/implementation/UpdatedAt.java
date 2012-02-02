package ggp.database.statistics.statistic.implementation;

import com.google.appengine.api.datastore.Entity;

import ggp.database.statistics.statistic.Statistic;

public class UpdatedAt extends Statistic {
    @Override public void updateWithMatch(Entity newMatch) {};
    @Override public Object getFinalForm() {
        return getStateVariable("updatedAt");
    }
    @Override public void finalizeComputation(Statistic.Reader theReader) {
        setStateVariable("updatedAt", System.currentTimeMillis());
    }
}