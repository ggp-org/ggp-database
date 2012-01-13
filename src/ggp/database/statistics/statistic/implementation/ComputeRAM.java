package ggp.database.statistics.statistic.implementation;

import com.google.appengine.api.datastore.Entity;

import ggp.database.statistics.statistic.Statistic;

public class ComputeRAM extends Statistic {
    @Override public void updateWithMatch(Entity newMatch) {};
    @Override public Object getFinalForm() {
        return getStateVariable("computeRAM");
    }
    @Override public void finalizeComputation(Statistic.Reader theReader) {
        if (getStateVariable("computeRAM") != 0) return;
        setStateVariable("computeRAM", Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory());
    }
}