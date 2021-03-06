import java.io.IOException;
import java.io.*;
import java.net.URI;
import java.util.Random;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class readHDFS {
    private FileSystem fs = null;
    private Configuration conf = null;
    private int cellSize;
    private int nodeSize;

    public readHDFS(int cellSize, int nodeSize) throws Exception{
        conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.1.105:9000");
        fs = FileSystem.get(new URI("hdfs://192.168.1.105:9000"),conf,"root");
        this.nodeSize = nodeSize;
        this.cellSize = cellSize;
    }

    public byte[] readFile(String pathName) throws Exception {
        byte[] fileData;
        ByteArrayOutputStream bOutput = new ByteArrayOutputStream(cellSize);
        FSDataInputStream in = fs.open(new Path(pathName));
        IOUtils.copyBytes(in, bOutput, cellSize);
        fileData = bOutput.toByteArray();
        in.close();
        return fileData;
    }

}
