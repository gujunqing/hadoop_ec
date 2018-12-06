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
    private int n;
    private int cellSize;
    private int nodeSize;
    private int nodeLen;
    private Random randomGenerator;
    public String prefix;

    public GenHDFSData(int n, int cellSize, int nodeSize) throws Exception{
        conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.1.112:9000");
        fs = FileSystem.get(new URI("hdfs://192.168.1.112:9000"),conf,"gua1s");
        randomGenerator = new Random(10);
        this.nodeSize = nodeSize;
        this.cellSize = cellSize;
        this.n = n;
        this.nodeLen = nodeSize/cellSize;
        assert nodeSize%cellSize == 0;
        prefix = String.valueOf(cellSize) + "_";
    }

    public byte[] genRanData(){
        byte[] genData = new byte[cellSize];
        for (int i=0; i < cellSize; i++) {
            genData[i] = (byte) randomGenerator.nextInt(256);
        }
        return genData;
    }

    public void makdir(String path) throws Exception {
        boolean mkdirs = fs.mkdirs(new Path(prefix+path));
        System.out.println(mkdirs);
    }

    private void deleteAll() throws Exception{
        for (int i=0; i<n; i++) {
            for (int j = 0; j < nodeLen; j++) {
                String pathName = String.valueOf(i);
                pathName += "/" + String.valueOf(j);
                boolean delete = fs.delete(new Path(pathName));
                // System.out.println(delete);
            }
        }
    }

    private void createFile(String pathName) throws Exception {
        FSDataOutputStream out = fs.create(new Path(pathName));
        byte[] inputData = genRanData();
        out.write(inputData);
        out.close();
    }

    public void createFile(String pathName, byte[] inputData) throws Exception {
        FSDataOutputStream out = fs.create(new Path(pathName));
        out.write(inputData);
        out.close();
    }

    private void fillFile() throws Exception {
        for (int i=0; i<n; i++) {
            for (int j=0; j<nodeLen; j++) {
                String pathName = prefix + String.valueOf(i);
                pathName += "/" + String.valueOf(j);
                createFile(pathName);
            }
        }
    }

    public static final void main(String[] args) throws Exception {
        int nodeSize = 128*1024*1024;
        int cellSize = 1024*16;
        int n = 6;
        GenHDFSData hdfs = new GenHDFSData(n, cellSize, nodeSize);
        hdfs.fillFile();
        // for (int i=0; i<n; i++) {
        //     hdfs.makdir(String.valueOf(i));
        // }

    }

}
