package ggp.database.statistics.statistic.implementation;

import ggp.database.statistics.statistic.Statistic;

public class ComputationTime extends Statistic {
    @Override public Object getFinalForm() {
        return getStateVariable("computeTime");
    }
    public void incrementComputeTime(double byTime) {
        incrementStateVariable("computeTime", byTime);   
    }
}