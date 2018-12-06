import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class hadoopEC_MSR {
    private MSRRawDecoder decoder;
    private MSRRawEncoder encoder;
    private Random randomGenerator;
    private ErasureCoderOptions coderOptions;
    private int n;
    private int k;
    private int l;
    private int cellsize;
    public int nodeSize;
    public int nodeLen;
    private byte[][] originData;
    private byte[][] parityData;
    private int[] erasedIndexes;

    public hadoopEC_MSR(int n, int k, int cellsize, int nodeSize) throws Exception {
        this.n = n;
        this.k = k;
        int m =n/(n-k);
        this.l = (int)Math.pow((n-k), m);
        this.cellsize = cellsize;
        coderOptions = new ErasureCoderOptions(k, n-k);
        originData = new byte[k*l][];
        parityData = new byte[(n-k)*l][];
        this.nodeSize = nodeSize;
        this.nodeLen = nodeSize/cellsize;
    }

    public void nodeEncode(String prefix) throws Exception {
        readHDFS hdfsRead = new readHDFS(cellsize, nodeSize);
        GenHDFSData hdfsWrite =new GenHDFSData(k, cellsize, nodeSize);
        for (int i=0; i<nodeLen/l; i++) {
            for (int j=0; j < k; j++) {
                for (int e=0; e < l; e++){
                    String pathName = prefix + String.valueOf(j) + "/" + String.valueOf(i*l+e);
                    originData[j*l+e] = hdfsRead.readFile(pathName);
                }
            }
            for (int j=0; j < (n-k)*l; j++)
                parityData[j] = new byte[cellsize];
            encodeStep();
            for (int j=0; j < (n-k); j++) {
                for (int e=0; e < l; e++){
                    String pathName = "MSR" + prefix + String.valueOf(j) + "/" + String.valueOf(i*l+e);
                    hdfsWrite.createFile(pathName, parityData[j*l+e]);
                }
            }
        }
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

    public static void nodeDecode(hadoopEC_MSR hec) throws Exception {
        int n = hec.n;
        int k = hec.k;
        int l = hec.l;
        int cellsize = hec.cellsize;
        int errorNum = 1;
        String prefix = String.valueOf(cellsize) + "_";
        hec.erasedIndexes = new int[errorNum];
        readHDFS hdfsRead = new readHDFS(cellsize, hec.nodeSize);
        GenHDFSData hdfsWrite =new GenHDFSData(k, cellsize, hec.nodeSize);
        for(int i = 0; i < errorNum; ++i)
            hec.erasedIndexes[i] = i;
        long sumTime = 0;
        long TsumTime = 0;
        long TsumData = 0;
        for (int i=0; i<hec.nodeLen/l; i++){
            byte[][] inputData = new byte[n*l][];
            long TstartTime = System.currentTimeMillis();
            int [][] dataIndexes = MSRRawDecoder.searchData(hec.erasedIndexes, n, k);
            for (int p=0; p < n; p++) {
                for (int j=0; j < l; j++){
                    if (dataIndexes[p][j] == 1){
                        if (p < k)
                            inputData[p*l+j] = hdfsRead.readFile(prefix+String.valueOf(p)+'/'+String.valueOf(i*l+j));
                        else
                            inputData[p*l+j] = hdfsRead.readFile("MSR"+prefix+String.valueOf(p-k)+'/'+String.valueOf(i*l+j));
                    } else {
                        inputData[p*l+j] = null;
                    }
                }
            }
            long TendTime = System.currentTimeMillis();
            TsumTime += TendTime - TstartTime;
            TsumData += k*cellsize;
            long startTime = System.currentTimeMillis();
            hec.decodeStep(inputData);
            long endTime = System.currentTimeMillis();
            sumTime += endTime-startTime;
        }
        System.out.println("The decoding time is：" + sumTime + "ms");
        System.out.println("The Transformation time is：" + TsumTime + "ms");
        System.out.println("The Transformation data is:" + TsumData/1024 + "KB");
    }

    public static final void main(String[] args) throws Exception {
        hadoopEC_MSR hec = new hadoopEC_MSR(12,6,1024*256, 128*1024*1024);
        int cellsize = hec.cellsize;
        String prefix = String.valueOf(cellsize) + "_";
        hec.nodeEncode(prefix);
    }

    private void encodeStep() throws IOException {
        encoder = new MSRRawEncoder(coderOptions);
        encoder.encode(originData, parityData);
    }

    private void decodeStep(byte[][] inputData) throws IOException {
        int len = erasedIndexes.length;
        byte[][] output = new byte[len*l][];
        for (int i=0; i < len*l; i++)
            output[i] = new byte[cellsize];
        decoder = new MSRRawDecoder(coderOptions);
        decoder.decode(inputData, erasedIndexes, output);
        // checkStep(output);
    }

    private void checkStep(byte[][] output) {
        int len = erasedIndexes.length;
        byte[][] realoutput = new byte[len*l][];
        for (int i=0; i < len; i++){
            int errorTip = erasedIndexes[i];
            for (int j=0; j<l; j++){
                if (errorTip < k)
                    realoutput[i*l+j] = Arrays.copyOf(originData[errorTip*l+j], cellsize);
                else
                    realoutput[i*l+j] = Arrays.copyOf(parityData[(errorTip-k)*l+j], cellsize);
            }
        }
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
