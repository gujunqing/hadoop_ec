import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import util.DumpUtil;

public class hadoopEC {
    private MSRRawDecoder decoder;
    private MSRRawEncoder encoder;
    private Random randomGenerator;
    private ErasureCoderOptions coderOptions;
    private int n;
    private int k;
    private int l;
    private int cellsize;
    private byte[][] originData;
    private byte[][] parityData;
    private int[] erasedIndexes;

    public hadoopEC() {
        randomGenerator = new Random(10);
        setParameter();
        coderOptions = new ErasureCoderOptions(k, n-k);
        originData = new byte[k*l][];
        parityData = new byte[(n-k)*l][];
        for (int i=0; i < k*l; i++)
            originData[i] = new byte[cellsize];
        for (int i=0; i < (n-k)*l; i++)
            parityData[i] = new byte[cellsize];
        for (int i=0; i < k*l; i++) {
            for (int j=0; j < cellsize; j++) {
                originData[i][j] = (byte)randomGenerator.nextInt(256);
            }
        }
    }

    private void setParameter() {
        n = 6;
        k = 3;
        l = 9;
        cellsize = 100;
    }

    private static void printMatrix(int[][] matrix, int row, int col) {
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

    private static void printMatrix(byte[][] matrix, int row, int col) {
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

    public static final void main(String[] args) throws IOException {
        hadoopEC hec = new hadoopEC();
        hec.encodeStep();
        byte[][] originData = hec.originData;
        byte[][] parityData = hec.parityData;
        int n = hec.n;
        int k = hec.k;
        int l = hec.l;
        int cellsize = hec.cellsize;
        hec.erasedIndexes = new int[1];
        hec.erasedIndexes[0] = 1;
        // hec.erasedIndexes[1] = 3;
        int [][] dataIndexes = MSRRawDecoder.searchData(hec.erasedIndexes, n, k);
        hec.printMatrix(dataIndexes, n, l);
        byte[][] inputData = new byte[n*l][];
        for (int i=0; i < n; i++) {
            for (int j=0; j < l; j++){
                if (dataIndexes[i][j] == 1){
                    if (i < k)
                        inputData[i*l+j] = Arrays.copyOf(originData[i*l+j], cellsize);
                    else
                        inputData[i*l+j] = Arrays.copyOf(parityData[(i-k)*l+j], cellsize);
                } else {
                    inputData[i*l+j] = null;
                }
            }
        }
        // hec.printMatrix(inputData, n*l, 10);

        hec.decodeStep(inputData);
    }

    private void encodeStep() throws IOException {
        encoder = new MSRRawEncoder(coderOptions);
        encoder.encode(originData, parityData);
    }

    private void decodeStep(byte[][] inputData) throws IOException {
        int len = erasedIndexes.length;
        byte[][] output = new byte[len*l][];
        byte[][] realoutput = new byte[len*l][];
        for (int i=0; i < len*l; i++)
            output[i] = new byte[cellsize];
        for (int i=0; i < len; i++){
            int errorTip = erasedIndexes[i];
            for (int j=0; j<l; j++){
                if (errorTip < k)
                    realoutput[i*l+j] = Arrays.copyOf(originData[errorTip*l+j], cellsize);
                else
                    realoutput[i*l+j] = Arrays.copyOf(parityData[(errorTip-k)*l+j], cellsize);
            }
        }
        decoder = new MSRRawDecoder(coderOptions);
        decoder.decode(inputData, erasedIndexes, output);
        checkStep(output, realoutput);
    }

    private void checkStep(byte[][] output, byte[][] realoutput) {
        int len = erasedIndexes.length;
        // printMatrix(output, len*l, 1);
        System.out.println();
        // printMatrix(realoutput, len*l, 1);
        for (int i=0; i<len*l; i++) {
            if (!Arrays.equals(output[i], realoutput[i])){
                System.out.println("Not equal");
                return;
            }
        }
        System.out.println("Equal");
    }

}
