package part1;

import java.io.*;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class part1 {
    public static void main(String[] args) throws Exception{
        String dst = args[0];
        String[] urlAddress = new String[6];
        urlAddress[0] = "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2";
        urlAddress[1] = "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2";
        urlAddress[2] = "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2";
        urlAddress[3] = "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2";
        urlAddress[4] = "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2";
        urlAddress[5] = "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2";

        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);

        for(int i=0; i<urlAddress.length; i++){
            String[] name = urlAddress[i].split("/");
            dst = dst + "/" + name[name.length - 1].trim();

            URL url = new URL(urlAddress[i]);
            InputStream in = url.openStream();

            InputStream inCompress = null;
            OutputStream out = null;

            Path inputPath = new Path(dst);
            CompressionCodec codec = factory.getCodec(inputPath);
            String outputUri = CompressionCodecFactory.removeSuffix(dst, codec.getDefaultExtension());

            //start compress the file
            if (codec == null) {
                System.err.println("No codec found for " + dst);
                System.exit(1);
            }

            FileSystem fs = FileSystem.get(URI.create(dst), conf);
            try {
                out = fs.create(inputPath);
                IOUtils.copyBytes(in, out, 4096, true);

                inCompress = codec.createInputStream(fs.open(inputPath));
                out = fs.create(new Path(outputUri));
                IOUtils.copyBytes(inCompress, out, conf);

                fs.delete(inputPath, true);

            } finally {
                IOUtils.closeStream(in);
                IOUtils.closeStream(inCompress);
                IOUtils.closeStream(out);
            }

            dst = args[0];
        }
    }
}
