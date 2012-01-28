package ggp.database.statistics.statistic.implementation;

import java.util.HashSet;
import java.util.Set;

import ggp.database.statistics.counters.WeightedAverage;
import ggp.database.statistics.statistic.CounterStatistic;
import ggp.database.statistics.statistic.PerGameStatistic;
import ggp.database.statistics.statistic.PerPlayerStatistic;
import ggp.database.statistics.statistic.PerRoleStatistic;
import ggp.database.statistics.statistic.WeightedAverageStatistic;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.repackaged.com.google.common.base.Pair;

public class RoleCorrelationWithSkill extends PerGameStatistic<PerRoleStatistic<CounterStatistic.NaiveCounter>> {
    public void updateWithMatch(Entity newMatch) {}
    
    public void finalizeComputation(Reader theReader) {        
        AgonRank AGON = theReader.getStatistic(AgonRank.class);
        RolePlayerAverageScore RPAS = theReader.getStatistic(RolePlayerAverageScore.class);
        
        for (String aGame : RPAS.getKnownGameNames()) {
            for (int i = 0; i < RPAS.getPerGameStatistic(aGame).getKnownRoleCount(); i++) {
                PerPlayerStatistic<WeightedAverageStatistic.NaiveWeightedAverage> PAS = RPAS.getPerGameStatistic(aGame).getPerRoleStatistic(i);
                Set<Pair<Double,Double>> dataPoints = new HashSet<Pair<Double,Double>>();
                WeightedAverage meanScore = new WeightedAverage();
                WeightedAverage meanSkill = new WeightedAverage();
                for (String aPlayer : PAS.getKnownPlayerNames()) {
                    double theScore = PAS.getPerPlayerStatistic(aPlayer).getWeightedAverage();
                    double theSkill = AGON.getPerPlayerStatistic(aPlayer).getValue();
                    dataPoints.add(new Pair<Double,Double>(theScore, theSkill));
                    meanScore.addValue(theScore);
                    meanSkill.addValue(theSkill);                    
                }
                double xBar = meanScore.getWeightedAverage();
                double yBar = meanSkill.getWeightedAverage();
                double A=0, B=0, C=0;
                for (Pair<Double,Double> aPoint : dataPoints) {
                    A += (aPoint.first - xBar)*(aPoint.second - yBar);
                    B += (aPoint.first - xBar)*(aPoint.first - xBar);
                    C += (aPoint.second - yBar)*(aPoint.second - yBar);
                }
                double theCorrelation = A/Math.sqrt(B*C);
                if (Double.isInfinite(theCorrelation) || Double.isNaN(theCorrelation)) {
                    theCorrelation = -1;
                }
                
                getPerGameStatistic(aGame).getPerRoleStatistic(i).incrementCounter(theCorrelation);
            }
        }
    }

    @Override
    protected PerRoleStatistic<CounterStatistic.NaiveCounter> getInitialStatistic() {
        class X1 extends PerRoleStatistic<CounterStatistic.NaiveCounter> {
            public void updateWithMatch(Entity newMatch) {}
            @Override protected CounterStatistic.NaiveCounter getInitialStatistic() {
                return new CounterStatistic.NaiveCounter();
            };
        }
        return new X1();
    }
}