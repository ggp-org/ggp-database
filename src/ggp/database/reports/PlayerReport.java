package ggp.database.reports;

import ggp.database.Persistence;
import ggp.database.matches.CondensedMatch;
import ggp.database.statistics.statistic.WeightedAverageStatistic;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import javax.jdo.Query;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import java.util.Properties;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.AddressException;

public class PlayerReport {
	private static double trimNumber(double x) {
		return Math.round(x*100)/100.0;
	}
	
    public static void generateReportFor(String thePlayer) throws IOException {        
        StringBuilder queryFilter = new StringBuilder();
        queryFilter.append("playerNamesFromHost == '" + thePlayer + "' && ");
        queryFilter.append("startTime > " + (System.currentTimeMillis() - 604800000L));
        
        Query query = Persistence.getPersistenceManager().newQuery(CondensedMatch.class);
        query.setFilter(queryFilter.toString());
        
        int nMatches = 0;
        WeightedAverageStatistic.NaiveWeightedAverage myAvgScore = new WeightedAverageStatistic.NaiveWeightedAverage();
        WeightedAverageStatistic.NaiveWeightedAverage myAvgErrors = new WeightedAverageStatistic.NaiveWeightedAverage();
        
        try {
            @SuppressWarnings("unchecked")
			List<CondensedMatch> results = (List<CondensedMatch>) query.execute();
            for (CondensedMatch e : results) {            	
            	int nRole = e.playerNamesFromHost.indexOf(thePlayer);
            	if (!e.isCompleted) continue;
            	
            	if (e.hasErrorsForPlayer.get(nRole)) {
            		myAvgErrors.addEntry(1.0, 1.0);
            	} else {
            		myAvgScore.addEntry(e.goalValues.get(nRole), 1.0);
            		myAvgErrors.addEntry(0.0, 1.0);
            	}
            	nMatches++;
            }
        } finally {
            query.closeAll();
        }
        
        StringBuilder theMessage = new StringBuilder();
        theMessage.append("Daily activity report for player " + thePlayer + ", generated on " + new Date() + ".\n");
        theMessage.append("Counts all activity over the past seven days.\n\n");
        theMessage.append("Total matches: " + nMatches + "\n");
        theMessage.append("Percentage with errors: " + trimNumber(myAvgErrors.getWeightedAverage()*100) + "%\n");
        theMessage.append("Avg score, for clean matches: " + trimNumber(myAvgScore.getWeightedAverage()) + "\n");
        
        Properties props = new Properties();
        Session session = Session.getDefaultInstance(props, null);
        
        try {
            Message msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress("noreply@ggp-database.appspotmail.com", "GGP.org Reporting"));
            msg.addRecipient(Message.RecipientType.TO, new InternetAddress("action@ggp.org", "GGP.org Client"));
            msg.setSubject("GGP.org Daily Report for " + thePlayer + " on " + new Date());
            msg.setText(theMessage.toString());
            Transport.send(msg);
        } catch (AddressException e) {
            throw new RuntimeException(e);
        } catch (MessagingException e) {
        	throw new RuntimeException(e);
        }
    }
}