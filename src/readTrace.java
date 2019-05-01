import io.netty.buffer.ByteBuf;
import org.omg.CORBA.MARSHAL;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.HashMap;

public class readTrace {
    private HashMap<Integer, Double> mapProbab = new HashMap<Integer, Double>();
    private HashMap<Integer, Boolean> mapHot = new HashMap<Integer, Boolean>();
    private HashMap<Integer, Integer> mapRead = new HashMap<Integer, Integer>();
    private HashMap<Integer, Integer> mapWrite = new HashMap<Integer, Integer>();
    private HashMap<Integer, Boolean> ismsr = new HashMap<Integer, Boolean>();
    private Random randomGenerator = new Random(10);
    private ErasureCoderOptions coderOptions;
    private int totalBlock;
    private GenHDFSData hdfsWrite;
    private readHDFS hdfsRead;
    private MSRNewDecoder MSRdecoder;
    private MSRNewEncoder MSRencoder;
    private RSRawDecoder RSdecoder;
    private RSRawEncoder RSencoder;
    public int k;
    public int r;
    public int l;
    public int cellSize;
    public int nodeSize;

    public readTrace(int cellSize, int nodeSize) throws Exception {
        this.cellSize = cellSize;
        this.nodeSize = nodeSize;
        k = 2;
        r = 2;
        int m = (k+r)/r;
        l = (int) Math.pow(r, m);
        this.totalBlock = nodeSize;
        int fileSize = k*l*cellSize;
        // Create read and write API
        hdfsWrite = new GenHDFSData(cellSize, nodeSize);
        hdfsRead = new readHDFS(cellSize, nodeSize);
        // Create encoder and decoder
        coderOptions = new ErasureCoderOptions(k, r);
        RSencoder = new RSRawEncoder(coderOptions);
        RSdecoder = new RSRawDecoder(coderOptions);
        MSRencoder = new MSRNewEncoder(coderOptions);
        MSRdecoder = new MSRNewDecoder(coderOptions);
        prepareMap();
        hdfsWrite.fillFile(k, l);
    }

    public void prepareMap() throws Exception {
        String mapPath = "/Users/gujunqing/Desktop/ali_ec_code/paper_xie/ecExp/hashMap.txt";
        try {
            FileReader fr = new FileReader(mapPath);
            BufferedReader br = new BufferedReader(fr);
            String line = "";
            String[] arrs = null;
            while ((line=br.readLine())!=null) {
                arrs = line.split(",");
                int blockNo = Integer.valueOf(arrs[0]);
                int isHot = Integer.valueOf(arrs[1]);
                int readNum = Integer.valueOf(arrs[2]);
                int writeNum = Integer.valueOf(arrs[3]);
                double prop = Double.valueOf(arrs[4]);
                mapProbab.put(blockNo, prop);
                mapHot.put(blockNo, (isHot == 1));
                mapRead.put(blockNo, readNum);
                mapWrite.put(blockNo, writeNum);
                ismsr.put(blockNo, false);
            }
            br.close();
            fr.close();
        } catch (IOException e) {
            System.out.println(e.toString());
            throw e;
        }
    }

    public byte[][] MSRencodeStep(byte[][] input) {
        MSRencoder = new MSRNewEncoder(coderOptions);
        byte[][] output = new byte[r][];
        for (int i=0; i<r; i++) {
            output[i] = new byte[cellSize*l];
        }
        ByteArrayEncodingState bbeState = new ByteArrayEncodingState(MSRencoder, input, output);

        MSRencoder.doEncode(bbeState);
        return output;
    }

    public byte[][] MSRdecodeStep(byte[][] input, int errorno) {
        int[] erasedIndexes = new int[1];
        erasedIndexes[0] = errorno;
        byte[][] output = new byte[1][];
        output[0] = new byte[cellSize*l];
        MSRdecoder = new MSRNewDecoder(coderOptions);
        ByteArrayDecodingState decodingState = new ByteArrayDecodingState(MSRdecoder,
                input, erasedIndexes, output);
        MSRdecoder.doDecode(decodingState);
        // checkStep(output);
        return output;
    }

    public byte[][] RSencodeStep(byte[][] input) throws Exception {
        RSdecoder = new RSRawDecoder(coderOptions);
        byte[][] output = new byte[r][];
        for (int i=0; i<r; i++) {
            output[i] = new byte[cellSize*l];
        }
        RSencoder = new RSRawEncoder(coderOptions);
        RSencoder.encode(input, output);
        return output;

    }

    public byte[][] RSdecodeStep(byte[][] input, int errorno) throws Exception {
        int[] erasedIndexes = new int[1];
        erasedIndexes[0] = errorno;
        byte[][] output = new byte[1][];
        output[0] = new byte[cellSize*l];
        RSdecoder = new RSRawDecoder(coderOptions);
        RSdecoder.decode(input, erasedIndexes, output);
        return output;
    }

    public void doExec(String[] arrs) throws Exception {
        int blockno = Integer.valueOf(arrs[2]) % totalBlock;
        int rwFlag = Integer.valueOf(arrs[4]);
        byte[][] input;
        byte[][] output;
        if (rwFlag == 1) {
            // Is read operation
            int ran = randomGenerator.nextInt(1000);
            if (ran < mapProbab.get(blockno)*1000) {
                // the block is wrong
                input = new byte[k+r][];
                // output = new byte [1][];
                // output[0] = new byte[cellSize*l];
                int errorno = randomGenerator.nextInt(k);
                if (ismsr.get(blockno)) {
                    // Is use msr code
                    int[][] formatArray = MSRNewDecoder.requiredDataFormat(errorno, k+r, k);
                    for (int i=0; i<k; i++) {
                        input[i] = new byte[cellSize*l];
                        for (int j=0; j<l; j++) {
                            if (formatArray[i][j] == 1) {
                                String filePath = "/expr/data" + String.valueOf(i) + "/" + String.valueOf(i) + "_" +
                                        String.valueOf(j);
                                byte[] tmp = hdfsRead.readFile(filePath);
                                System.arraycopy(tmp, 0, input[i], j * cellSize, cellSize);
                            }
                        }
                    }
                    for (int i=0; i<r; i++) {
                        input[k+i] = new byte[cellSize*l];
                        for (int j=0; j<l; j++) {
                            if (formatArray[i][j] == 1) {
                                String filePath = "/expr/parity" + String.valueOf(i) + "/" + String.valueOf(i) + "_" +
                                        String.valueOf(j);
                                byte[] tmp = hdfsRead.readFile(filePath);
                                System.arraycopy(tmp, 0, input[k+i], j * cellSize, cellSize);
                            }
                        }
                    }
                    output = MSRdecodeStep(input, errorno);
                } else {
                    // Is use RS code
                    for (int i=0; i<k; i++) {
                        if (errorno == i) continue;
                        input[i] = new byte[cellSize*l];
                        for (int j=0; j<l; j++) {
                            String filePath = "/expr/data" + String.valueOf(i) + "/" + String.valueOf(i) + "_" +
                                    String.valueOf(j);
                            byte[] tmp = hdfsRead.readFile(filePath);
                            System.arraycopy(tmp, 0, input[k+i], j * cellSize, cellSize);
                        }
                    }
                    input[k] = new byte[cellSize*l];
                    for (int j=0; j<l; j++) {
                        String filePath = "/expr/parity" + String.valueOf(0) + "/" + String.valueOf(0) + "_" +
                                String.valueOf(j);
                        byte[] tmp = hdfsRead.readFile(filePath);
                        System.arraycopy(tmp, 0, input[k], j * cellSize, cellSize);
                    }
                    for (int i=1; i<r; i++) {
                        input[k+i] = null;
                    }
                    output = RSdecodeStep(input, errorno);
                }

            } else {
                // the block is right
                for (int i=0; i<k; i++) {
                    for (int j=0; j<l; j++) {
                        String filePath = "/expr/data" + String.valueOf(i) + "/" + String.valueOf(i) + "_" +
                                String.valueOf(j);
                        hdfsRead.readFile(filePath);
                    }
                }
            }
        } else {
            // Is write operation
            if (mapHot.get(blockno) && !ismsr.get(blockno)) {
                if (mapRead.get(blockno) > mapWrite.get(blockno) && mapProbab.get(blockno) > 0.05) {
                    ismsr.put(blockno, true);
                }
            }
            input = new byte[k][];
            for (int i=0; i<k; i++) {
                input[i] = new byte[cellSize*l];
            }
            // write data
            for (int i=0; i<k; i++) {
                for (int j=0; j<l; j++) {
                    byte[] tmp = hdfsWrite.genRanData();
                    String filePath = "/expr/data" + String.valueOf(i) + "/" + String.valueOf(i) + "_" +
                            String.valueOf(j);
                    hdfsWrite.createFile(filePath, tmp);
                    System.arraycopy(tmp, 0, input[i], cellSize*j, cellSize);
                }
            }
            if (ismsr.get(blockno)) {
                // do msr encode
                output = MSRencodeStep(input);
            } else {
                // do rs encode
                output = RSencodeStep(input);
            }
            // write parity
            for (int i=0; i<k; i++) {
                for (int j=0; j<l; j++) {
                    byte[] tmp = new byte[cellSize];
                    System.arraycopy(output[i], cellSize*j, tmp, 0, cellSize);
                    String filePath = "/expr/parity" + String.valueOf(i) + "/" + String.valueOf(i) + "_" +
                            String.valueOf(j);
                    hdfsWrite.createFile(filePath, tmp);
                }
            }
        }
    }

    public void prepareRScode() throws Exception {
        byte[][] input = new byte[k][];
        for (int i=0; i<k; i++) {
            input[i] = new byte[cellSize*l];
        }
        for (int e=0; e<nodeSize; e++) {
            // read data
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < l; j++) {
                    String filePath = "/expr/data" + String.valueOf(i) + "/" + String.valueOf(e) + "_" +
                            String.valueOf(j);
                    byte[] tmp = hdfsRead.readFile(filePath);
                    System.arraycopy(tmp, 0, input[i], cellSize * j, cellSize);
                }
            }
            byte[][] output = RSencodeStep(input);
            // write parity
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < l; j++) {
                    byte[] tmp = new byte[cellSize];
                    System.arraycopy(output[i], cellSize * j, tmp, 0, cellSize);
                    String filePath = "/expr/parity" + String.valueOf(i) + "/" + String.valueOf(e) + "_" +
                            String.valueOf(j);
                    hdfsWrite.createFile(filePath, tmp);
                }
            }
        }
    }

    public void traceReader(String tracePath) throws Exception {
        File file = new File("/Users/gujunqing/Desktop/testFile");
        try {
            FileReader fr = new FileReader(tracePath);
            BufferedReader br = new BufferedReader(fr);
            String line = "";
            String[] arrs = null;
            while ((line=br.readLine())!=null) {
                arrs = line.split("\t| ");
                doExec(arrs);
            }
            br.close();
            fr.close();
        } catch (IOException e) {
            System.out.println(e.toString());
            throw e;
        }

    }

    public static final void main(String[] args) throws Exception {
        int cellSize = 1024*1024;
        int blockNum = 20000;
        readTrace rt = new readTrace(cellSize, blockNum);
        rt.prepareRScode();
    }

}
