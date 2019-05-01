import java.io.IOException;
import java.util.Arrays;
import java.util.Random;


// for more details for pom.xml read
// share/doc/hadoop/hadoop-mapreduce-client/hadoop-mapreduce-client-core/dependency-analysis.html

public class hadoopEC_RS {
    private RSRawDecoder RSdecoder;
    private RSRawEncoder RSencoder;
    // private Random randomGenerator;
    private ErasureCoderOptions coderOptions;
    private int n;
    private int k;
    private int cellsize;
    private int nodeSize;
    private int nodeLen;
    private byte[][] originData;
    private byte[][] parityData;
    private int[] erasedIndexes;

    public hadoopEC_RS(int n, int k, int cellsize, int nodeSize) throws Exception {
        // randomGenerator = new Random(10);
        setRSParameter(n, k, cellsize);
        this.nodeSize = nodeSize;
        this.nodeLen = nodeSize/cellsize;
        coderOptions = new ErasureCoderOptions(k, n-k, false);
        originData = new byte[k][];
        parityData = new byte[(n-k)][];
    }

    public void nodeEncode(String prefix) throws Exception {
        readHDFS hdfsRead = new readHDFS(cellsize, nodeSize);
        GenHDFSData hdfsWrite =new GenHDFSData(cellsize, nodeSize);
        for (int i=0; i<nodeLen; i++) {
            for (int j=0; j < k; j++) {
                String pathName = prefix + String.valueOf(j) + "/" + String.valueOf(i);
                originData[j] = hdfsRead.readFile(pathName);
            }
            for (int j=0; j < (n-k); j++)
                parityData[j] = new byte[cellsize];
            RSencodeStep();
            for (int j=0; j < (n-k); j++) {
                String pathName = "RS" + prefix + String.valueOf(j) + "/" + String.valueOf(i);
                hdfsWrite.createFile(pathName, parityData[j]);
            }
        }
    }

    private void setRSParameter(int n, int k, int cellsize) {
        this.n = n;
        this.k = k;
        this.cellsize = cellsize;
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

    public static void nodeDecode(hadoopEC_RS hec) throws Exception {
        int n = hec.n;
        int k = hec.k;
        int r = n-k;
        int cellsize = hec.cellsize;
        int errorNum = 1;
        String prefix = String.valueOf(cellsize) + "_";
        hec.erasedIndexes = new int[errorNum];
        readHDFS hdfsRead = new readHDFS(cellsize, hec.nodeSize);
        GenHDFSData hdfsWrite =new GenHDFSData(cellsize, hec.nodeSize);
        for(int i = 0; i < errorNum; ++i)
            hec.erasedIndexes[i] = i;

        long sumTime = 0;
        long TsumTime = 0;
        long TsumData = 0;
        for (int i=0; i<hec.nodeLen; i++){
            byte[][] inputData = new byte[n][];
            int t = 0;
            int idx = 0;
            long TstartTime = System.currentTimeMillis();
            for (int j=0; j<n; j++) {
                if (t < errorNum && j == hec.erasedIndexes[t]){
                    t++;
                    inputData[j] = null;
                    continue;
                }
                if (idx >= k) {
                    inputData[j] = null;
                    continue;
                }
                if (j<k)
                    inputData[j] = hdfsRead.readFile(prefix+String.valueOf(j)+'/'+String.valueOf(i));
                else
                    inputData[j] = hdfsRead.readFile("RS"+prefix+String.valueOf(j-k)+'/'+String.valueOf(i));
                idx++;
            }
            long TendTime = System.currentTimeMillis();
            TsumTime += TendTime - TstartTime;
            TsumData += k*cellsize;
            int calLen = 1024*1024;
            for (int p = 0; p<calLen ; p++) {
                int mlen = cellsize/calLen;
                byte[][] mInput = new byte[n][];
                for (int ii=0; ii<n; ii++){
                    if (inputData[ii] == null)
                        mInput[ii] = null;
                    else {
                        byte[] tmp = new byte[mlen];
                        System.arraycopy(inputData[ii], mlen*p, tmp, 0, mlen);
                        mInput[ii] = tmp;
                    }
                }
                long startTime = System.currentTimeMillis();
                hec.RSdecodeStep(mInput);
                long endTime = System.currentTimeMillis();
                sumTime += endTime - startTime;
            }
        }
        System.out.println("The decoding time is：" + sumTime + "ms");
        System.out.println("The Transformation time is：" + TsumTime + "ms");
        System.out.println("The Transformation data is:" + TsumData/1024 + "KB");
    }

    public static final void main(String[] args) throws Exception {
        hadoopEC_RS hec = new hadoopEC_RS(26,24,1024*1024*4, 512*1024*1024);
        int cellsize = hec.cellsize;
        String prefix = String.valueOf(cellsize) + "_";
        hec.nodeEncode(prefix);
    }

    private void RSencodeStep() throws IOException {
        RSencoder = new RSRawEncoder(coderOptions);
        RSencoder.encode(originData, parityData);
    }

    private void RSdecodeStep(byte[][] inputData) throws IOException {
        int len = erasedIndexes.length;
        byte[][] output = new byte[len][];
        for (int i=0; i < len; i++)
            output[i] = new byte[cellsize];
        RSdecoder = new RSRawDecoder(coderOptions);
        RSdecoder.decode(inputData, erasedIndexes, output);
        // checkStep(output);
    }

    private void checkStep(byte[][] output) {
        int len = erasedIndexes.length;
        byte[][] realoutput = new byte[len][];
        for (int i=0; i < len; i++){
            int errorTip = erasedIndexes[i];
            if (errorTip < k)
                realoutput[i] = Arrays.copyOf(originData[errorTip], cellsize);
            else
                realoutput[i] = Arrays.copyOf(parityData[(errorTip-k)], cellsize);
        }
        printMatrix(output, len, 10);
        System.out.println();
        printMatrix(realoutput, len, 10);
        for (int i=0; i<len; i++) {
            if (!Arrays.equals(output[i], realoutput[i])){
                System.out.println("Not equal");
                return;
            }
        }
        System.out.println("Equal");
    }

}
