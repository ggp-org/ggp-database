package ggp.database.reports;

import org.ggp.galaxy.shared.persistence.Persistence;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.annotations.*;

@PersistenceCapable
public class DailyReport {
    @SuppressWarnings("unused")
    @PrimaryKey @Persistent private String thePrimaryKey;
    @Persistent private Set<String> recipients;    
    @Persistent private ReportType reportType;
    @Persistent private String reportOn;

    public enum ReportType {
    	HOST, PLAYER,
    }
    
    private DailyReport(String reportOn, ReportType reportType) {
        thePrimaryKey = getPrimaryKey(reportOn, reportType);
        recipients = new HashSet<String>();
        this.reportType = reportType;
        this.reportOn = reportOn;
    }
    
    public void generateReport() throws IOException {
    	if (reportType == ReportType.HOST) {
    		HostReport.generateReportFor(reportOn, recipients);
    	} else if (reportType == ReportType.PLAYER) {
    		PlayerReport.generateReportFor(reportOn, recipients);
    	}
    }
    
    public void registerEmail(String email) {
    	recipients.add(email);
    	save();
    }

    public void save() {
        PersistenceManager pm = Persistence.getPersistenceManager();
        pm.makePersistent(this);
        pm.close();
    }

    /* Static accessor methods */
    public static DailyReport loadDailyReport(String reportOn, ReportType reportType) throws IOException {
        DailyReport theReport = Persistence.loadSpecific(getPrimaryKey(reportOn, reportType), DailyReport.class);
        if (theReport == null) {
        	theReport = new DailyReport(reportOn, reportType);
        }
        return theReport;
    }
    
    private static String getPrimaryKey(String reportOn, ReportType reportType) {
    	return "" + reportType + ":" + reportOn;
    }
}