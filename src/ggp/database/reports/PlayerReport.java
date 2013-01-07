package ggp.database.reports;

import org.ggp.shared.persistence.Persistence;
import ggp.database.matches.CondensedMatch;
import ggp.database.statistics.statistic.WeightedAverageStatistic;

import java.io.IOException;
import java.util.Collection;
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
	
    public static void generateReportFor(String thePlayer, Collection<String> toAddresses) throws IOException {
    	if (toAddresses.size() == 0) return;
    	
        StringBuilder queryFilter = new StringBuilder();
        queryFilter.append("playerNamesFromHost == '" + thePlayer + "' && ");
        queryFilter.append("startTime > " + (System.currentTimeMillis() - 604800000L));
        
        Query query = Persistence.getPersistenceManager().newQuery(CondensedMatch.class);
        query.setFilter(queryFilter.toString());
        
        int nMatches = 0;
        long latestCleanStartTime = 0;
        WeightedAverageStatistic.NaiveWeightedAverage myAvgScore = new WeightedAverageStatistic.NaiveWeightedAverage();
        WeightedAverageStatistic.NaiveWeightedAverage myAvgErrors = new WeightedAverageStatistic.NaiveWeightedAverage();
        
        try {
            @SuppressWarnings("unchecked")
			List<CondensedMatch> results = (List<CondensedMatch>) query.execute();
            for (CondensedMatch e : results) {            	
            	int nRole = e.playerNamesFromHost.indexOf(thePlayer);
            	if (!e.isCompleted) continue;
            	
            	boolean hasErrors = e.hasErrorsForPlayer.get(nRole);
            	myAvgErrors.addEntry(hasErrors ? 1 : 0, 1.0);            	
            	if (!hasErrors) {
            		// Some stats are computed only over clean matches.
            		myAvgScore.addEntry(e.goalValues.get(nRole), 1.0);
                	if (e.startTime > latestCleanStartTime) {
                		latestCleanStartTime = e.startTime;
                	}
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
        theMessage.append("Last clean match started on " + new Date(latestCleanStartTime) + "\n");
        
        Properties props = new Properties();
        Session session = Session.getDefaultInstance(props, null);
        
        for (String toAddress : toAddresses) {
	        try {
	            Message msg = new MimeMessage(session);
	            msg.setFrom(new InternetAddress("noreply-reporting@ggp-database.appspotmail.com", "GGP.org Reporting"));
	            msg.addRecipient(Message.RecipientType.TO, new InternetAddress(toAddress));
	            msg.setSubject("GGP.org Daily Report for player " + thePlayer + " on " + new Date());
	            msg.setText(theMessage.toString());
	            Transport.send(msg);
	        } catch (AddressException e) {
	            throw new RuntimeException(e);
	        } catch (MessagingException e) {
	        	throw new RuntimeException(e);
	        }
        }
    }
}