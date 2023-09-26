import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/*CS550 Advanced Operating Systems Programming Assignment 1 Repo
Illinois Institute of Technology

Team Name: KK Students:

Anirudha Kapileshwari (akapileshwari@hawk.iit.edu)
Mugdha Atul Kulkarni (mkulkarni2@hawk.iit.edu) */


/*
 * This class Handles all the log files operations 
 * downlaod.log
 * replica.log
 */

public class LogMaker {

    private String logFileName = "";
    private BufferedWriter logWriter = null;
    private final String logDirectory = "logs/";

    public LogMaker(String logType) {
        try {
            if (logType.equalsIgnoreCase("Peer")) {
                logFileName = "download.log";
            } else if (logType.equalsIgnoreCase("Server")) {
                logFileName = "server.log";
            } else if (logType.equalsIgnoreCase("Replication")) {
                logFileName = "replication.log";
            }
            
            // Create a logs folder if it doesn't exist
            File directory = new File(logDirectory);
            if (!directory.exists()) {
                directory.mkdirs();
            }

            logWriter = new BufferedWriter(new FileWriter(logDirectory + logFileName, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    public boolean wlog(String logText) {
        boolean isLogSuccessful = false;
        try {
            String timestamp = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss").format(Calendar.getInstance().getTime());
            if (logWriter != null) {
                logText = String.format("[%s] %s", timestamp, logText);
                logWriter.write(logText);
                logWriter.newLine();
                isLogSuccessful = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return isLogSuccessful;
    }
    //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    public void printLogs() {
        BufferedReader reader = null;
        int charCount = 0;

        System.out.println("\nLOGS");
        System.out.println("=========================================================================");

        try {
            reader = new BufferedReader(new FileReader(logDirectory + logFileName));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                charCount += line.length();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (charCount == 0) {
            System.out.println("NO LOGS TO PRINT");
        }

        System.out.println("=========================================================================");
    }

    //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	public void closeLogger() {
        try {
            if (logWriter != null) {
                logWriter.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
