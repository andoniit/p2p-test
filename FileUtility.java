import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;


/*CS550 Advanced Operating Systems Programming Assignment 1 Repo
Illinois Institute of Technology

Team Name: KK Students:

Anirudha Kapileshwari (akapileshwari@hawk.iit.edu)
Mugdha Atul Kulkarni (mkulkarni2@hawk.iit.edu) */



//file method handling class


public class FileUtility {

    private static final String DOWNLOAD_LOCATION = "downloads/";
    private static final String REPLICA_LOCATION = "replica/";
    private static final int BUFFER_SIZE = 1024 * 64; // 64 KiloBytes

    // Method to get a list of files in a directory
    public static ArrayList<String> getFiles(String path) {
        ArrayList<String> files = new ArrayList<>();
        File folder = new File(path);

        if (folder.isDirectory()) {
            File[] listOfFiles = folder.listFiles();

            if (listOfFiles != null) {
                for (File file : listOfFiles) {
                    if (file.isFile() && !file.getName().endsWith("~")) {
                        files.add(file.getName());
                    }
                }
            }
        } else if (folder.isFile()) {
            files.add(path.substring(path.lastIndexOf("/") + 1));
        }

        return files;
    }
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Method to get the location of a file in a list of locations
    public static String getFileLocation(String fileName, List<String> locations) {
        for (String path : locations) {
            File folder = new File(path);
            File[] listOfFiles = folder.listFiles();

            for (File file : listOfFiles) {
                if (file.getName().equals(fileName)) {
                    return path.endsWith("/") ? path : path.concat("/");
                }
            }
        }

        return "File Not Found.";
    }
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Method to download a file from a remote host
    public static boolean downloadFile(String hostAddress, int port, String fileName) {
        InputStream in = null;
        BufferedOutputStream fileOutput = null;
        ObjectOutputStream out = null;
        Socket socket = null;
        boolean isDownloaded = false;

        try {
            socket = new Socket(hostAddress, port);
            System.out.println("\nDownloading file " + fileName);

            File downloadFolder = new File(DOWNLOAD_LOCATION);
            if (!downloadFolder.exists()) {
                downloadFolder.mkdir();
            }

            out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();

            System.out.println("Requesting file.........");

            Request request = new Request();
            request.setRequestType("DOWNLOAD");
            request.setRequestData(fileName);
            out.writeObject(request);

            System.out.println("Downloading file........");
            byte[] buffer = new byte[BUFFER_SIZE];
            in = socket.getInputStream();
            fileOutput = new BufferedOutputStream(new FileOutputStream(DOWNLOAD_LOCATION + fileName));

            int bytesRead;
            while ((bytesRead = in.read(buffer, 0, buffer.length)) > 0) {
                fileOutput.write(buffer, 0, bytesRead);
            }

            isDownloaded = new File(DOWNLOAD_LOCATION + fileName).length() > 0;
            if (!isDownloaded) {
                new File(DOWNLOAD_LOCATION + fileName).delete();
            }
        } catch (SocketException e) {
            isDownloaded = false;
        } catch (Exception e) {
            isDownloaded = false;
        } finally {
            closeStreams(socket, out, in, fileOutput);
        }

        return isDownloaded;
    }
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Method to replicate a file from a remote host
    public static boolean replicateFile(String hostAddress, int port, String fileName) {
        InputStream in = null;
        BufferedOutputStream fileOutput = null;
        ObjectOutputStream out = null;
        Socket socket = null;
        boolean isReplicated = false;
        LogUtility log = new LogUtility("replication");

        try {
            long startTime = System.currentTimeMillis();
            socket = new Socket(hostAddress, port);

            File replicaFolder = new File(REPLICA_LOCATION);
            if (!replicaFolder.exists()) {
                replicaFolder.mkdir();
            }

            out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();

            log.wlog("Requesting file ... " + fileName);

            Request request = new Request();
            request.setRequestType("DOWNLOAD");
            request.setRequestData(fileName);
            out.writeObject(request);

            log.wlog("Downloading file ... " + fileName);
            byte[] buffer = new byte[BUFFER_SIZE];
            in = socket.getInputStream();
            fileOutput = new BufferedOutputStream(new FileOutputStream(REPLICA_LOCATION + fileName));

            int bytesRead;
            while ((bytesRead = in.read(buffer, 0, buffer.length)) > 0) {
                fileOutput.write(buffer, 0, bytesRead);
            }

            long endTime = System.currentTimeMillis();
            double time = (double) Math.round(endTime - startTime) / 1000;
            log.wlog("File downloaded successfully in " + time + " seconds.");
            isReplicated = true;
        } catch (SocketException e) {
            log.wlog("Unable to connect to the host. Unable to download file.");
            isReplicated = false;
            log.wlog("Error:" + e);
        } catch (Exception e) {
            log.wlog("Unable to download file. Please check if you have write permission.");
            isReplicated = false;
            log.wlog("Error:" + e);
        } finally {
            closeStreams(socket, out, in, fileOutput);
            if (log != null) {
                log.closeLogger();
            }
        }

        return isReplicated;
    }
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Method to print the contents of a downloaded file
    public static void printFile(String fileName) {
        File file = new File(DOWNLOAD_LOCATION + fileName);

        if (file.exists()) {
            System.out.println("\nFile Downloaded**********");
            System.out.println("AND THE CONTENTS OF THE FILE ARE BELOW. PRINTING ONLY FIRST 2000 CHARACTERS.");
            System.out.println("=========================================================================");
            BufferedReader br = null;
            int charCount = 0;

            try {
                br = new BufferedReader(new FileReader(DOWNLOAD_LOCATION + fileName));
                String line = null;

                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                    charCount += line.length();
                    if (charCount > 2000) break;
                }
            } catch (Exception e) {
                System.out.println("Unable to print file.");
                System.out.println("Error:" + e);
            } finally {
                try {
                    if (br != null) br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            System.out.println("=========================================================================");
        } else {
            System.out.println("\nThe file could not be printed because it may not have been downloaded.");
        }
    }
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Helper method to close streams and socket
    private static void closeStreams(Socket socket, ObjectOutputStream out, InputStream in, BufferedOutputStream fileOutput) {
        try {
            if (out != null) out.close();
            if (in != null) in.close();
            if (fileOutput != null) fileOutput.close();
            if (socket != null) socket.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
