import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Future;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class bufferTest {
    private Random randGen = new Random();
    private int cellsize = 1024*1024;
    private byte[] genData(int cellsize) {
        byte[] tmp = new byte[cellsize];
        for (int i=0; i<cellsize; i++) {
            tmp[i] = (byte)randGen.nextInt(256);
        }
        return tmp;
    }

    public byte[] getDataNode() {
        return genData(cellsize);
    }
    public bufferTest(int size) {
        BufferedOutputStream bos = null;
        FileOutputStream fos = null;
        File file = null;
        file = new File("/Users/gujunqing/Desktop/testFile");
        try {
            fos = new FileOutputStream(file);
            bos = new BufferedOutputStream(fos);
            for (int i=0; i<1024; i++) {
                byte[] bfile = getDataNode();
                bos.write(bfile);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        int cellsize = 1024*1024*2;
        readHDFS r = new readHDFS(cellsize, 128*cellsize);
        r.readFile("/test/testFile");

        // testCase ec = new testCase(n, k, cellSize);

    }
}
