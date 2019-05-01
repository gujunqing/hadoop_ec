import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class hadoopEC_XOR {
    private int n;
    private int k;
    private int cellsize;
    public int nodeSize;
    public int nodeLen;
    private byte[][] originData;
    private byte[][] parityData;
    private int[] erasedIndexes;
    private ErasureCoderOptions coderOptions;

    public hadoopEC_XOR(int n, int k, int cellsize, int nodeSize) throws Exception {
        // randomGenerator = new Random(10);
        setRSParameter(n, k, cellsize);
        this.nodeSize = nodeSize;
        this.nodeLen = nodeSize/cellsize;
        coderOptions = new ErasureCoderOptions(k, n-k, false);
        originData = new byte[k][];
        parityData = new byte[(n-k)][];
    }

    private void setRSParameter(int n, int k, int cellsize) {
        this.n = n;
        this.k = k;
        this.cellsize = cellsize;
    }

    public void nodeEncode(String prefix) throws Exception {
        readHDFS hdfsRead = new readHDFS(cellsize, nodeSize);
        GenHDFSData hdfsWrite =new GenHDFSData(k, cellsize);
        for (int i=0; i<nodeLen; i++) {
            for (int j=0; j < k; j++) {
                String pathName = prefix + String.valueOf(j) + "/" + String.valueOf(i);
                originData[j] = hdfsRead.readFile(pathName);
            }
            for (int j=0; j < (n-k); j++)
                parityData[j] = new byte[cellsize];
            XORencodeStep();
            for (int j=0; j < (n-k); j++) {
                String pathName = "XOR" + prefix + String.valueOf(j) + "/" + String.valueOf(i);
                hdfsWrite.createFile(pathName, parityData[j]);
            }
        }
    }

    private void XORencodeStep() throws IOException {
        for (int j=0; j<cellsize; j++) {
            byte temp=0;
            for (int i=0; i<k; i++){
                temp ^= originData[i][j];
            }
            parityData[0][j] = temp;
        }
    }

    public static final void main(String[] args) throws Exception {
        hadoopEC_XOR hec = new hadoopEC_XOR(5,4,1024*1024*256, 128*1024*1024);
        int cellsize = hec.cellsize;
        String prefix = String.valueOf(cellsize) + "_";
        hec.nodeEncode(prefix);
        nodeDecode(hec);
    }

    public static void nodeDecode(hadoopEC_XOR hec) throws Exception {
        int n = hec.n;
        int k = hec.k;
        int r = n-k;
        int cellsize = hec.cellsize;
        int errorNum = 1;
        String prefix = String.valueOf(cellsize) + "_";
        hec.erasedIndexes = new int[errorNum];
        readHDFS hdfsRead = new readHDFS(cellsize, hec.nodeSize);
        GenHDFSData hdfsWrite =new GenHDFSData(k, cellsize);
        hec.erasedIndexes[0] = 0;

        long sumTime = 0;
        long TsumTime = 0;
        long TsumData = 0;
        for (int i=0; i<hec.nodeLen; i++){
            byte[][] inputData = new byte[n][];
            long TstartTime = System.currentTimeMillis();
            for (int j=0; j<n; j++) {
                if (j == hec.erasedIndexes[0]){
                    inputData[j] = null;
                } else {
                    if (j < k)
                        inputData[j] = hdfsRead.readFile(prefix + String.valueOf(j) + '/' + String.valueOf(i));
                    else
                        inputData[j] = hdfsRead.readFile("XOR" + prefix + String.valueOf(j - k) + '/' + String.valueOf(i));
                }
            }
            long TendTime = System.currentTimeMillis();
            TsumTime += TendTime - TstartTime;
            TsumData += k*cellsize;
            long startTime = System.currentTimeMillis();
            hec.XORdecodeStep(inputData);
            long endTime = System.currentTimeMillis();
            sumTime += endTime-startTime;
        }
        System.out.println("The decoding time is：" + sumTime + "ms");
        System.out.println("The Transformation time is：" + TsumTime + "ms");
        System.out.println("The Transformation data is:" + TsumData/1024 + "KB");
    }

    private void XORdecodeStep(byte[][] inputData) {
        int len = 1;
        byte[][] outputData = new byte[len][];
        for (int i=0; i < len; i++)
            outputData[i] = new byte[cellsize];
        for (int j=0; j<cellsize; j++) {
            byte temp=0;
            for (int i=0; i<k; i++){
                if (k==erasedIndexes[0]) continue;
                temp ^= inputData[i][j];
            }
            outputData[0][j] = temp;
        }
    }
}
