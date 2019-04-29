import com.google.common.base.Preconditions;
import util.DumpUtil;
import util.GF256;
import util.RSUtil;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class MSRNewDecoder extends RawErasureDecoder {
    //relevant to schema and won't change during decode calls
    private byte[] MSRMatrix;

    /**
     * Below are relevant to schema and erased indexes, thus may change during
     * decode calls.
     */
    private byte[] decodeMatrix;
    /**
     * Array of input tables generated from coding coefficients previously.
     * Must be of size 32*k*rows
     */
    private int dataLen;
    private int l;
    private byte[] gfTables;
    private int[] validIndexes;
    private boolean[] erasureFlags;
    private int[] cachedErasedIndexes;
    private int numErasedDataUnits;
    private final int sliceUnit = 1024;


    public MSRNewDecoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);

        int numAllUnits = getNumAllUnits();
        if (getNumAllUnits() >= RSUtil.GF.getFieldSize()) {
            System.out.println(
                    "Invalid getNumDataUnits() and numParityUnits");
        }
        assert getNumAllUnits() % getNumParityUnits() == 0;
        int m = getNumAllUnits() / getNumParityUnits();
        int s = getNumParityUnits();
        this.l = (int)Math.pow(s, m);
        MSRMatrix = new byte[getNumParityUnits() * getNumAllUnits() * l * l];
        RSUtil.genMSRMatrix(MSRMatrix, numAllUnits, getNumDataUnits());
        if (allowVerboseDump()) {
            DumpUtil.dumpMatrix(MSRMatrix, getNumParityUnits()*l, getNumAllUnits()*l);
        }
    }

    public int getSpel() {
        return this.l;
    }

    protected void doDecode(ByteBufferDecodingState decodingState) {
        Preconditions.checkState(decodingState.decodeLength%sliceUnit == 0);
        CoderUtil.resetOutputBuffers(decodingState.outputs,
                decodingState.decodeLength);
        ByteBuffer[] realInputs = new ByteBuffer[getNumAllUnits()*l];
        ByteBuffer[] realOutputs = new ByteBuffer[decodingState.erasedIndexes.length*l];
        ByteBuffer[] noNullRealInputs;
        int realEncodeLength = decodingState.decodeLength/l;
        int slice = sliceUnit/l;
        boolean single = (decodingState.erasedIndexes.length == 1);
        for (int i=0; i<decodingState.erasedIndexes.length*l; i++) {
            realOutputs[i] = ByteBuffer.allocate(realEncodeLength);
        }
        if (single) {
            int[][] format = MSRNewDecoder.requiredDataFormat(decodingState.erasedIndexes[0]
                    , getNumAllUnits(), getNumDataUnits());
            for (int i=0; i < getNumAllUnits(); i++) {
                for (int j=0; j<l; j++) {
                    if (format[i][j] == 1) {
                        realInputs[i*l+j] = ByteBuffer.allocate(realEncodeLength);
                        for (int k = 0; k < decodingState.decodeLength / sliceUnit; k++) {
                            int pos = k * sliceUnit + j * slice;
                            decodingState.inputs[i].limit(pos + slice);
                            decodingState.inputs[i].position(pos);
                            realInputs[i * l + j].put(decodingState.inputs[i].duplicate());
                        }
                    }
                    else {
                        realInputs[i*l+j] = null;
                    }
                }
            }
        } else {
            for (int i=0; i < getNumAllUnits(); i++) {
                if (decodingState.inputs[i] == null) {
                    for (int j=0; j<l; j++) realInputs[i*l+j] = null;
                    continue;
                }
                for (int j=0; j<l; j++) {
                    realInputs[i*l+j] = ByteBuffer.allocate(realEncodeLength);
                    for (int k=0; k<decodingState.decodeLength/sliceUnit; k++) {
                        int pos = k*sliceUnit+j*slice;
                        decodingState.inputs[i].limit(pos+slice);
                        decodingState.inputs[i].position(pos);
                        // int realPos = k*slice;
                        realInputs[i*l+j].put(decodingState.inputs[i].duplicate());
                    }
                }
            }
        }
        // Prepare the decode matrix and gftable
        prepareDecoding(realInputs, decodingState.erasedIndexes);
        // Start to decode
        if (single) {
            Preconditions.checkArgument(validIndexes.length == (getNumAllUnits()-1)*l/getNumParityUnits());
            noNullRealInputs = new ByteBuffer[(getNumAllUnits()-1)*l/getNumParityUnits()];
            for (int i = 0; i < (getNumAllUnits()-1)*l/getNumParityUnits(); i++) {
                noNullRealInputs[i] = realInputs[validIndexes[i]];
                noNullRealInputs[i].flip();
                noNullRealInputs[i].limit(realEncodeLength);
            }
            RSUtil.encodeData(gfTables, noNullRealInputs, realOutputs);
        } else {
            Preconditions.checkArgument(validIndexes.length == getNumDataUnits()*l);
            noNullRealInputs = new ByteBuffer[getNumDataUnits()*l];
            for (int i = 0; i < getNumDataUnits()*l; i++) {
                noNullRealInputs[i] = realInputs[validIndexes[i]];
            }
            RSUtil.encodeData(gfTables, noNullRealInputs, realOutputs);
        }

        for (int i=0; i<decodingState.erasedIndexes.length; i++) {
            //int idx = decodingState.erasedIndexes[i];
            for (int k=0; k<decodingState.decodeLength/sliceUnit; k++) {
                for (int j=0; j<l; j++) {
                    // int pos = k*sliceUnit+j*slice;
                    int realPos = k*slice;
                    realOutputs[i*l+j].limit(realPos+slice);
                    realOutputs[i*l+j].position(realPos);
                    decodingState.outputs[i].put(realOutputs[i*l+j].duplicate());
                }
            }
        }
    }

    @Override
    protected void doDecode(ByteArrayDecodingState decodingState) {
        Preconditions.checkState(decodingState.decodeLength%sliceUnit == 0);
        CoderUtil.resetOutputBuffers(decodingState.outputs,
                decodingState.outputOffsets, dataLen);
        byte[][] realInputs = new byte[getNumAllUnits()*l][];
        byte[][] realOutputs = new byte[decodingState.erasedIndexes.length*l][];
        byte[][] noNullRealInputs;
        int realEncodeLength = decodingState.decodeLength/l;
        int slice = sliceUnit/l;
        boolean single = (decodingState.erasedIndexes.length == 1);
        for (int i=0; i<decodingState.erasedIndexes.length*l; i++) {
            realOutputs[i] = new byte[realEncodeLength];
        }
        if (single) {
            int[][] format = MSRNewDecoder.requiredDataFormat(decodingState.erasedIndexes[0]
                    , getNumAllUnits(), getNumDataUnits());
            for (int i=0; i < getNumAllUnits(); i++) {
                for (int j=0; j<l; j++) {
                    if (format[i][j] == 1) {
                        realInputs[i*l+j] = new byte[realEncodeLength];
                        for (int k = 0; k < decodingState.decodeLength / sliceUnit; k++) {
                            int pos = k * sliceUnit + j * slice;
                            System.arraycopy(decodingState.inputs[i], pos, realInputs[i*l+j], k*slice, slice);
                        }
                    }
                    else {
                        realInputs[i*l+j] = null;
                    }
                }
            }
        } else {
            for (int i=0; i < getNumAllUnits(); i++) {
                if (decodingState.inputs[i] == null) {
                    for (int j=0; j<l; j++) realInputs[i*l+j] = null;
                    continue;
                }
                for (int j=0; j<l; j++) {
                    realInputs[i*l+j] = new byte[realEncodeLength];
                    for (int k=0; k<decodingState.decodeLength/sliceUnit; k++) {
                        int pos = k*sliceUnit+j*slice;
                        System.arraycopy(decodingState.inputs[i], pos, realInputs[i*l+j], k*slice, slice);
                    }
                }
            }
        }

        // Prepare the decode matrix and gftable
        prepareDecoding(realInputs, decodingState.erasedIndexes);
        // Start to decode
        if (single) {
            Preconditions.checkArgument(validIndexes.length == (getNumAllUnits()-1)*l/getNumParityUnits());
            int[] realInputOffsets = new int[(getNumAllUnits()-1)*l/getNumParityUnits()];
            int[] realOutputOffsets = new int[l];
            noNullRealInputs = new byte[(getNumAllUnits()-1)*l/getNumParityUnits()][];
            for (int i = 0; i < (getNumAllUnits()-1)*l/getNumParityUnits(); i++) {
                noNullRealInputs[i] = realInputs[validIndexes[i]];
                // realInputOffsets[i] = decodingState.inputOffsets[validIndexes[i]];
            }
            RSUtil.encodeData(gfTables, realEncodeLength, noNullRealInputs, realInputOffsets,
                    realOutputs, realOutputOffsets);
        } else {
            Preconditions.checkArgument(validIndexes.length == getNumDataUnits()*l);
            int[] realInputOffsets = new int[getNumDataUnits()*l];
            int[] realOutputOffsets = new int[decodingState.erasedIndexes.length*l];
            noNullRealInputs = new byte[getNumDataUnits()*l][];
            for (int i = 0; i < getNumDataUnits()*l; i++) {
                noNullRealInputs[i] = realInputs[validIndexes[i]];
            }
            RSUtil.encodeData(gfTables, realEncodeLength, noNullRealInputs, realInputOffsets,
                    realOutputs, realOutputOffsets);
        }

        for (int i=0; i<decodingState.erasedIndexes.length; i++) {
            //int idx = decodingState.erasedIndexes[i];
            for (int k=0; k<decodingState.decodeLength/sliceUnit; k++) {
                for (int j=0; j<l; j++) {
                    // int pos = k*sliceUnit+j*slice;
                    System.arraycopy(realOutputs[i*l+j], k*slice, decodingState.outputs[i],
                            k*sliceUnit+j*slice, slice);
                }
            }
        }
    }

    private <T> void prepareDecoding(T[] inputs, int[] erasedIndexes) {
        int[] tmpValidIndexes = CoderUtil.getValidIndexes(inputs);
        if (Arrays.equals(this.cachedErasedIndexes, erasedIndexes) &&
                Arrays.equals(this.validIndexes, tmpValidIndexes)) {
            return; // Optimization. Nothing to do
        }
        this.cachedErasedIndexes =
                Arrays.copyOf(erasedIndexes, erasedIndexes.length);
        this.validIndexes =
                Arrays.copyOf(tmpValidIndexes, tmpValidIndexes.length);

        processErasures(erasedIndexes);
    }

    private void processErasures(int[] erasedIndexes) {
        this.erasureFlags = new boolean[getNumAllUnits()];
        this.numErasedDataUnits = 0;

        for (int i = 0; i < erasedIndexes.length; i++) {
            int index = erasedIndexes[i];
            erasureFlags[index] = true;
            if (index < getNumDataUnits()) {
                numErasedDataUnits++;
            }
        }
        if (erasedIndexes.length == 1) {
            this.gfTables = new byte[(getNumAllUnits()-1)*l/getNumParityUnits() * l * 32];
            singlePrepareDecoding(erasedIndexes);
            RSUtil.initTables((getNumAllUnits()-1)*l/getNumParityUnits(), l,
                    decodeMatrix, 0, gfTables);
        } else {
            this.gfTables = new byte[erasedIndexes.length*l * getNumDataUnits()*l * 32];
            mulPrepareDecoding(erasedIndexes);
            RSUtil.initTables(getNumDataUnits()*l, erasedIndexes.length*l,
                    decodeMatrix, 0, gfTables);
        }
        if (allowVerboseDump()) {
            System.out.println(DumpUtil.bytesToHex(gfTables, -1));
        }
    }

    // generate the decode matrix for multi errors
    private void mulPrepareDecoding(int[] erasedIndexes) {
        int n = getNumAllUnits();
        int k = getNumDataUnits();
        int r = n - k;
        int s = r;
        int m = n / s;
        int l = (int)Math.pow(s, m);
        int i, j, p, q;
        r = erasedIndexes.length;
        int rr = n-k-r;
        byte tar, inv, tmp;

        // generate decode matrix
        this.decodeMatrix = new byte[r*l*k*l];
        // this.gfTables = new byte[r*l * k*l * 32];

        // DumpUtil.dumpMatrix(MSRMatrix, r*l, n*l);
        for (i=0; i < r; i++) {
            int idx = erasedIndexes[i];
            for (j=0; j < l; j++){
                tar = MSRMatrix[(i*l+j) * n*l + idx*l+j];
                if (tar != 1){
                    inv = GF256.gfInv(tar);
                    for (p = 0; p < (n-rr)*l; p++)
                        MSRMatrix[(i*l+j) * l*n + p] = GF256.gfMul(MSRMatrix[(i*l+j) * l*n + p], inv);
                }
                for (p = 0; p < r*l; p++) {
                    if (p == (i*l+j))  continue;
                    tmp = MSRMatrix[p * l*n + idx*l+j];
                    for (q = 0; q < (n-rr)*l; q++)
                        MSRMatrix[p * l*n + q] ^= GF256.gfMul(MSRMatrix[(i*l+j) * l*n + q], tmp);
                }
            }
        }
        // Divide the PCM matrix
        int pos, t;
        for (i = 0; i < r*l; i++) {
            pos = i*n*l;
            for (j = 0; j < k*l; j++) {
                t = validIndexes[j];
                decodeMatrix[k*l * i + j] = MSRMatrix[pos + t];
            }
        }
    }

    private void singlePrepareDecoding(int[] erasedIndexes) {
        int i, j, p, e, w;
        int n = getNumAllUnits();
        int k = getNumDataUnits();
        int v, u;
        int a, b, temp;
        int r = n - k;
        int s = r;
        int m = n / s;
        int l = (int)Math.pow(s, m);
        int errorTip = erasedIndexes[0];

        // initialize
        int[] Ary = new int[m];
        int[] a_index = new int[l/s];
        u = errorTip % s;
        v = (errorTip-u)/s + 1;
        byte[] leftMatrix = new byte[l*l];
        byte[] invertleftMatrix = new byte[l*l];
        byte[] tmpMatrix = new byte[l*(n-1)*l/r];
        this.decodeMatrix = new byte[l*(n-1)*l/r];

        // a_v = u
        w = 0;
        for (a=0; a < l; a++) {
            e = 0;
            temp = a;
            for (p=0; p < m; p++) Ary[p] = 0;
            while (temp/s != 0) {
                Ary[e] = temp%s;
                temp = temp/s;
                e++;
            }
            Ary[e] = temp % s;
            if (Ary[v-1] == u) {
                a_index[w] = a;
                w++;
            }
        }

        // set the decode matrix valuye
        for (j=0; j < r; j++)
            for (a=0; a < l/s; a++){
                for (b=0; b < l; b++)
                    leftMatrix[j*(l/s)*l + a*l + b] = MSRMatrix[j*l*n*l + a_index[a]*n*l + errorTip*l + b];
                int pp=0;
                for (p=0; p < n; p++){
                    if (p == errorTip) {
                        continue;
                    }
                    for(b=0; b < l/r; b++){
                        tmpMatrix[j*l/s*(n-1)*l/s + a*(n-1)*l/s + pp*l/r + b] =
                                MSRMatrix[j*l*n*l + a_index[a]*n*l + p*l + a_index[b]];
                    }
                    pp++;
                }
            }
        // DumpUtil.dumpMatrix(tmpMatrix, l, (n-1)*l/r);
        GF256.gfInvertMatrix(leftMatrix, invertleftMatrix, l);
        // generate decode matrix
        byte tmp;
        for (i=0; i < l; i++)
            for (j=0; j < (n-1)*l/r; j++) {
                tmp = 0;
                for (e=0; e < l; e++)
                    tmp ^= GF256.gfMul(invertleftMatrix[i*l + e], tmpMatrix[e*(n-1)*l/r + j]);
                decodeMatrix[i*(n-1)*l/r + j] = tmp;
            }
    }

    public static int[][] requiredDataFormat(int erasedIdx, int n, int k){
        int i, j, e, p;
        int v, u;
        int r = n - k;
        int s = r;
        int m = n / s;
        int l = (int)Math.pow(s, m);
        int[][] dataIndexes = new int[n][];
        for (i=0; i < n; i++)
            dataIndexes[i] = new int[l];
        // single node recovery
        for (i=0; i < n; i++)
            for (j=0; j < n; j++)
                dataIndexes[i][j] = 0;
        u = erasedIdx % s;
        v = (erasedIdx-u)/s + 1;
        int Ary[] = new int[m];
        int temp;
        for (i=0; i < l; i++) {
            e = 0;
            temp = i;
            for (j=0; j < m; j++) Ary[j] = 0;
            while (temp/s != 0) {
                Ary[e] = temp % s;
                temp = temp / s;
                e++;
            }
            Ary[e] = temp % s;
            // compare a_v and u
            if (Ary[v-1] == u) {
                for (p=0; p < n; p++)
                    if (p != erasedIdx) dataIndexes[p][i] = 1;
            }
        }
        return dataIndexes;
    }
}




















