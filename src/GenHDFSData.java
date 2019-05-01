import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Random;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


public class GenHDFSData {
    private FileSystem fs = null;
    private Configuration conf = null;
    private int cellSize;
    private int nodeSize;
    private int nodeLen;
    private Random randomGenerator;
    public String prefix;

    public GenHDFSData(int cellSize, int nodeSize) throws Exception{
        conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.1.105:9000");
        fs = FileSystem.get(new URI("hdfs://192.168.1.105:9000"),conf,"root");
        randomGenerator = new Random(10);
        this.nodeSize = nodeSize;
        this.cellSize = cellSize;
    }

    public byte[] genRanData(){
        byte[] genData = new byte[cellSize];
        for (int i=0; i < cellSize; i++) {
            genData[i] = (byte) randomGenerator.nextInt(256);
        }
        return genData;
    }

    public void createFile(String pathName) throws Exception {
        FSDataOutputStream out = fs.create(new Path(pathName));
        byte[] inputData = genRanData();
        out.write(inputData);
        out.close();
    }

    public void deleteAll(String pathName) throws Exception {
        fs.delete(new Path(pathName), true);
    }

    public void createFile(String pathName, byte[] inputData) throws Exception {
        FSDataOutputStream out = fs.create(new Path(pathName));
        out.write(inputData);
        out.close();
    }

    public void fillFile(int k, int l) throws Exception {
        for (int i=0; i<nodeSize; i++) {
            for (int j=0; j<k; j++) {
                // String pathName = prefix + String.valueOf(i);
                for (int e=0; e<l; e++) {
                    String pathName = "/expr/data" + String.valueOf(j) + "/" + String.valueOf(i) + "_" +
                            String.valueOf(e);
                    createFile(pathName);
                }
            }
        }
    }

    public static final void main(String[] args) throws Exception {
        int nodeSize = 128;
        int cellSize = 1024;
        int k = 2;
        int r = 2;
        int m = (k+r)/r;
        int l = (int)Math.pow(r, m);
        GenHDFSData hdfs = new GenHDFSData(cellSize, nodeSize);
        hdfs.deleteAll("/expr/data");
        //hdfs.fillFile(2, l);
        // for (int i=0; i<n; i++) {
        //     hdfs.makdir(String.valueOf(i));
        // }

    }

}
