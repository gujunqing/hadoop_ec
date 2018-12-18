import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class testCase {

    private MSRRawDecoder MSRdecoder;
    private MSRRawEncoder MSRencoder;
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
        for(int i=0; i < k*l; i++) {
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
        originData = new byte[k*l][];
        parityData = new byte[r*l][];
        coderOptions = new ErasureCoderOptions(k, n-k, false);
        getDataNode();
        for (int i=0; i < r*l; i++)
            parityData[i] = new byte[cellsize];
    }

    private void MSRencodeStep() throws IOException {
        MSRencoder = new MSRRawEncoder(coderOptions);
        MSRencoder.encode(originData, parityData);
    }

    private void MSRdecodeStep(byte[][] inputData) throws IOException {
        int len = erasedIndexes.length*l;
        byte[][] output = new byte[len][];
        for (int i=0; i < len; i++)
            output[i] = new byte[cellsize];
        MSRdecoder = new MSRRawDecoder(coderOptions);
        MSRdecoder.decode(inputData, erasedIndexes, output);
        checkStep(output);
    }

    private byte[][] RSgetData() {
        int errorNum = 2;
        erasedIndexes = new int[errorNum];
        byte[][] inputData = new byte[n][];
        for(int i = 0; i < errorNum; ++i)
            erasedIndexes[i] = i;
        int t = 0;
        int idx = 0;
        for (int j=0; j<n; j++) {
            if (t < errorNum && j == erasedIndexes[t]){
                t++;
                inputData[j] = null;
                continue;
            }
            if (idx >= k) {
                inputData[j] = null;
                continue;
            }
            if (j<k)
                inputData[j] = originData[j];
            else
                inputData[j] = parityData[j-k];
            idx++;
        }
        return inputData;
    }

    private byte[][] MSRgetData() {
        int errorNum = 2;
        erasedIndexes = new int[errorNum];
        byte[][] inputData = new byte[n*l][];
        for(int i = 0; i < errorNum; ++i)
            erasedIndexes[i] = i;
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
        return inputData;
    }

    public static void main(String[] args) throws Exception {
        int n = 4;
        int k = 2;
        int cellSize = 1024;
        testCase ec = new testCase(n, k, cellSize);
        ec.MSRencodeStep();
        long TsumTime = 0;
        for (int i=0; i<1; i++) {
            long TstartTime = System.currentTimeMillis();
            byte[][] input = ec.MSRgetData();
            ec.MSRdecodeStep(input);
            long TendTime = System.currentTimeMillis();
            TsumTime += TendTime - TstartTime;
        }
        System.out.println(TsumTime);
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
        byte[][] realoutput = new byte[len*l][];
        for (int i=0; i < len; i++){
            int errorTip = erasedIndexes[i];
            if (errorTip < k) {
                for (int j = 0; j < l; j++)
                    realoutput[i * l + j] = originData[errorTip * l + j];
            }
            else {
                for (int j = 0; j < l; j++)
                    realoutput[i * l + j] = parityData[(errorTip - k) * l + j];
            }
        }
        // printMatrix(output, len, 10);
        // System.out.println();
        // printMatrix(realoutput, len, 10);
        for (int i=0; i<len*l; i++) {
            if (!Arrays.equals(output[i], realoutput[i])){
                System.out.println("Not equal");
                return;
            }
        }
        // System.out.println("Equal");
    }

}
