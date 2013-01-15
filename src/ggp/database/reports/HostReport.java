package ggp.database.reports;

import org.ggp.galaxy.shared.persistence.Persistence;
import ggp.database.matches.CondensedMatch;
import ggp.database.statistics.statistic.WeightedAverageStatistic;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.jdo.Query;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import java.util.Properties;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.AddressException;

public class HostReport {
	private static double trimNumber(double x) {
		return Math.round(x*100)/100.0;
	}
	
	private static String getHostHashedPK(String theHost) {
		if (theHost.toLowerCase().equals("tiltyard")) return "90bd08a7df7b8113a45f1e537c1853c3974006b2";
		if (theHost.toLowerCase().equals("dresden")) return "f69721b2f73839e513eed991e96824f1af218ac1";
		if (theHost.toLowerCase().equals("artemis")) return "5bc94f8e793772e8585a444f2fc95d2ac087fed0";
		if (theHost.toLowerCase().equals("sample")) return "0ca7065b86d7646166d86233f9e23ac47d8320d4";
		return theHost;
	}
	
    public static void generateReportFor(String theHost, Collection<String> toAddresses) throws IOException {
    	if (toAddresses.size() == 0) return;
    	
        StringBuilder queryFilter = new StringBuilder();
        queryFilter.append("hashedMatchHostPK == '" + getHostHashedPK(theHost) + "' && ");
        queryFilter.append("startTime > " + (System.currentTimeMillis() - 604800000L));
        
        Query query = Persistence.getPersistenceManager().newQuery(CondensedMatch.class);
        query.setFilter(queryFilter.toString());
        
        int nMatches = 0;
        Set<String> distinctGames = new HashSet<String>();
        Set<String> distinctPlayers = new HashSet<String>();
        long latestStartTime = 0;
        WeightedAverageStatistic.NaiveWeightedAverage playersPerMatch = new WeightedAverageStatistic.NaiveWeightedAverage();
        WeightedAverageStatistic.NaiveWeightedAverage fractionScrambled = new WeightedAverageStatistic.NaiveWeightedAverage();
        WeightedAverageStatistic.NaiveWeightedAverage fractionAbandoned = new WeightedAverageStatistic.NaiveWeightedAverage();

        try {
            @SuppressWarnings("unchecked")
			List<CondensedMatch> results = (List<CondensedMatch>) query.execute();
            for (CondensedMatch e : results) {
            	distinctGames.add(e.gameMetaURL);
            	distinctPlayers.addAll(e.playerNamesFromHost);
            	playersPerMatch.addEntry(e.matchRoles, 1.0);
            	fractionScrambled.addEntry((e.scrambled != null && e.scrambled) ? 1 : 0, 1.0);
            	fractionAbandoned.addEntry((!e.isCompleted && e.startTime < System.currentTimeMillis()-21600000L) ? 1 : 0, 1.0);
            	latestStartTime = Math.max(latestStartTime, e.startTime);
            	nMatches++;
            }
        } finally {
            query.closeAll();
        }
        
        StringBuilder theMessage = new StringBuilder();
        theMessage.append("Daily activity report for host " + theHost + ", generated on " + new Date() + ".\n");
        theMessage.append("Counts all activity over the past seven days.\n\n");
        theMessage.append("Total matches: " + nMatches + "\n");
        theMessage.append("Percentage abandoned: " + trimNumber(fractionAbandoned.getWeightedAverage()*100) + "%\n");
        theMessage.append("Percentage scrambled: " + trimNumber(fractionScrambled.getWeightedAverage()*100) + "%\n");
        theMessage.append("Average players/match: " + trimNumber(playersPerMatch.getWeightedAverage()) + "\n");
        theMessage.append("Unique 7DA players: " + distinctPlayers.size() + "\n");
        theMessage.append("Unique 7DA games: " + distinctGames.size() + "\n");
        theMessage.append("Last match started on " + new Date(latestStartTime) + "\n");
        
        Properties props = new Properties();
        Session session = Session.getDefaultInstance(props, null);
        
        for (String toAddress : toAddresses) {
	        try {
	            Message msg = new MimeMessage(session);
	            msg.setFrom(new InternetAddress("noreply-reporting@ggp-database.appspotmail.com", "GGP.org Reporting"));
	            msg.addRecipient(Message.RecipientType.TO, new InternetAddress(toAddress));
	            msg.setSubject("GGP.org Daily Report for host " + theHost + " on " + new Date());
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