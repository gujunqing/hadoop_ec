import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

public class testCase {

    private MSRNewDecoder MSRdecoder;
    private MSRNewEncoder MSRencoder;
    // private Random randomGenerator;
    private ErasureCoderOptions coderOptions;
    private Random randGen = new Random();
    private int n;
    private int k;
    private int l;
    private int cellsize;
    private byte[][] originData;
    private byte[][] parityData;
    private int[] erasedIndexes;

    private byte[] genData(int cellsize) {
        byte[] tmp = new byte[cellsize];
        for (int i=0; i<cellsize; i++) {
            tmp[i] = (byte)randGen.nextInt(256);
        }
        return tmp;
    }

    public void getDataNode() {
        for(int i=0; i < k; i++) {
            originData[i] = genData(cellsize);
        }
    }

    public testCase(int n, int k, int cellsize) {
        this.n = n;
        this.k = k;
        int r = n - k;
        int m = n/r;
        this.l = (int)Math.pow(r, m);
        this.cellsize = cellsize;
        this.originData = new byte[k][];
        this.parityData = new byte[r][];
        getDataNode();
        for (int i=0; i<r; i++) {
            parityData[i] = new byte[cellsize];
        }
        coderOptions = new ErasureCoderOptions(k, n-k);

    }

    private void MSRencodeStep() throws IOException {
        MSRencoder = new MSRNewEncoder(coderOptions);
        ByteArrayEncodingState bbeState = new ByteArrayEncodingState(MSRencoder, originData, parityData);

        MSRencoder.doEncode(bbeState);
    }

    private void MSRdecodeStep(byte[][] inputData) throws IOException {
        int len = erasedIndexes.length;
        byte[][] output = new byte[len][];
        for (int i=0; i < len; i++)
            output[i] = new byte[cellsize];
        MSRdecoder = new MSRNewDecoder(coderOptions);
        ByteArrayDecodingState decodingState = new ByteArrayDecodingState(MSRdecoder,
                inputData, erasedIndexes, output);
        MSRdecoder.doDecode(decodingState);
        checkStep(output);
    }

    private byte[][] MSRgetData() {
        int errorNum = 1;
        erasedIndexes = new int[errorNum];
        byte[][] inputData = new byte[n][];
        for(int i = 0; i < errorNum; ++i)
            erasedIndexes[i] = i;
        /*
        int[][] validIndex = MSRRawDecoder.searchData(erasedIndexes, n, k);
        for (int i=0; i < n; i++) {
            if (i < k) {
                for (int j = 0; j < l; j++) {
                    if (validIndex[i][j] == 1)
                        inputData[i*l + j] = originData[i * l + j];
                    else
                        inputData[i*l + j] = null;
                }
            } else {
                for (int j = 0; j < l; j++) {
                    if (validIndex[i][j] == 1)
                        inputData[i*l + j] = parityData[(i-k) * l + j];
                    else
                        inputData[i*l + j] = null;
                }
            }
        }
        */
        inputData[0] = null;
        inputData[1] = originData[1];
        inputData[2] = parityData[0];
        inputData[3] = parityData[1];
        /*
        for (int i=0; i<inputData.length; i++) {
            if (inputData[i] == null) continue;
            inputData[i].flip();
            inputData[i].limit(cellsize);
        }
        */
        return inputData;
    }

    public static void main(String[] args) throws Exception {
        int n = 4;
        int k = 2;
        int r = n-k;
        int cellSize = 1024*1024;
        testCase ec = new testCase(n, k, cellSize);

        ec.MSRencodeStep();

        byte[][] input = ec.MSRgetData();
        ec.MSRdecodeStep(input);
    }

    private void printMatrix(byte[][] matrix, int row, int col) {
        for (int i=0; i<row; i++) {
            if (matrix[i] == null) {
                System.out.println(" null");
                continue;
            }
            for (int j = 0; j < col; j++) {
                System.out.print(" ");
                System.out.print(matrix[i][j]);
            }
            System.out.println();
        }
    }

    private void checkStep(byte[][] output) {
        int len = erasedIndexes.length;
        byte[][] realoutput = new byte[len][];
        for (int i=0; i < len; i++){
            int errorTip = erasedIndexes[i];
            if (errorTip < k) {
                realoutput[i] = originData[errorTip];
            }
            else {
                realoutput[i] = parityData[(errorTip - k)];
            }
        }

        // printMatrix(output, len, 10);
        // System.out.println();
        // printMatrix(realoutput, len, 10);
        for (int i=0; i<len; i++) {
            if (!Arrays.equals(output[i], realoutput[i])){
                System.out.println("Not equal");
                return;
            }
        }
        // System.out.println("Equal");
    }

}
