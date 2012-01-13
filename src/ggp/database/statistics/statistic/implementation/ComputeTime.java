package ggp.database.statistics.statistic.implementation;

import com.google.appengine.api.datastore.Entity;

import ggp.database.statistics.statistic.Statistic;

public class ComputeTime extends Statistic {
    @Override public void updateWithMatch(Entity newMatch) {};
    @Override public Object getFinalForm() {
        return getStateVariable("computeTime");
    }
    public void incrementComputeTime(double byTime) {
        incrementStateVariable("computeTime", byTime);   
    }
}