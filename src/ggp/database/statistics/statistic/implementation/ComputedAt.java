package ggp.database.statistics.statistic.implementation;

import com.google.appengine.api.datastore.Entity;

import ggp.database.statistics.statistic.Statistic;

public class ComputedAt extends Statistic {
    @Override public void updateWithMatch(Entity newMatch) {};
    @Override public Object getFinalForm() {
        return getStateVariable("computedAt");
    }
    @Override public void finalizeComputation(Statistic.Reader theReader) {
        if (getStateVariable("computedAt") != 0) return;
        setStateVariable("computedAt", System.currentTimeMillis());
    }
}