package ggp.database.statistics.statistic.implementation;

import com.google.appengine.api.datastore.Entity;

import ggp.database.statistics.statistic.Statistic;

public class StatsVersion extends Statistic {
    @Override public void updateWithMatch(Entity newMatch) {};
    @Override public Object getFinalForm() {
        return 27;
    }
}