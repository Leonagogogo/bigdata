package part1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


public class part2 {
    private static final int BUFFER_SIZE = 4096;

    public static void main(String[] args) {
        String fileURL = args[0];
        String saveDir = args[1];

        try {
            downloadFile(fileURL, saveDir);
            decompressFile(fileURL, saveDir);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void downloadFile(String fileURL, String saveDir) throws IOException {
        URL url = new URL(fileURL);
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        int responseCode = httpConn.getResponseCode();

        if (responseCode == HttpURLConnection.HTTP_OK) {
            String fileName[] = fileURL.split("/");
            String saveFilePath = saveDir + File.separator + fileName[fileName.length - 1].trim();

            InputStream inputStream = httpConn.getInputStream();
            FileSystem fs = FileSystem.get(URI.create(saveFilePath), new Configuration());
            OutputStream outputStream = fs.create(new Path(saveFilePath));

            IOUtils.copyBytes(inputStream, outputStream, 4096, true);

            outputStream.close();
            inputStream.close();

            System.out.println("File downloaded");
        } else {
            System.out.println("No file to download. Server replied HTTP code: " + responseCode);
        }
        httpConn.disconnect();
    }

    public static void decompressFile(String fileURL, String saveDir) throws IOException {
        String fileName[] = fileURL.split("/");
        String name = fileName[fileName.length - 1].trim();
        String saveFilePath = saveDir + File.separator + name;
        int index = name.indexOf(".");
        String finalFilePath = saveDir + File.separator + name.substring(0, index);

        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));

        Path zipPath = new Path(saveFilePath);
        FileSystem zipFile = FileSystem.get(conf);

        FileSystem fsIn = FileSystem.get(URI.create(saveFilePath), conf);
        InputStream in = fsIn.open(zipPath);
        ZipInputStream zipInput = new ZipInputStream(in);
        ZipEntry entry = null;

        Path folderPath = new Path(finalFilePath);
        FileSystem folderFile = FileSystem.get(conf);
        if(folderFile.exists(folderPath)) {
            folderFile.delete(folderPath,true);
        }

        OutputStream out = null;
        int bytesRead = -1;
        byte[] buffer = new byte[BUFFER_SIZE];
        while((entry = zipInput.getNextEntry()) != null){
            String finalPath = finalFilePath + File.separator + entry.getName();
            FileSystem fsOut = FileSystem.get(URI.create(finalPath), new Configuration());
            out = fsOut.create(new Path(finalPath));

            while ((bytesRead = zipInput.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }


        out.close();
        zipInput.close();
        in.close();
        zipFile.delete(zipPath,true);
    }
}