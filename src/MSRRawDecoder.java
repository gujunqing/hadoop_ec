/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import util.DumpUtil;
import util.GF256;
import util.RSUtil;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A raw erasure decoder in MSR code scheme in pure Java in case native one
 * isn't available in some environment. Please always use native implementations
 * when possible. This new Java coder is about 5X faster than the one originated
 * from HDFS-RAID, and also compatible with the native/ISA-L coder.
 */

public class MSRRawDecoder extends RawErasureDecoder {
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
    // private byte[] gfTables;
    private int dataLen;
    private byte[] gfTables;
    private int[] validIndexes;
    private int numErasedDataUnits;
    private boolean[] erasureFlags;

    public MSRRawDecoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);

        int numAllUnits = getNumAllUnits();
        if (getNumAllUnits() >= RSUtil.GF.getFieldSize()) {
            System.out.println(
                    "Invalid getNumDataUnits() and numParityUnits");
        }
        // generate the encodeMatrix
        assert getNumAllUnits() % getNumParityUnits() == 0;
        int m = getNumAllUnits() / getNumParityUnits();
        int s = getNumParityUnits();
        int l = (int)Math.pow(s, m);
        MSRMatrix = new byte[getNumParityUnits() * getNumAllUnits() * l * l];
        RSUtil.genMSRMatrix(MSRMatrix, numAllUnits, getNumDataUnits());
        // DumpUtil.dumpMatrix(MSRMatrix, getNumParityUnits()*l, getNumAllUnits()*l);

    }

    @Override
    protected void doDecode(ByteArrayDecodingState decodingState) {
        dataLen = decodingState.decodeLength;
        int n = getNumAllUnits();
        int k = getNumDataUnits();
        int r = n - k;
        int s = r;
        int m = n / s;
        int l = (int)Math.pow(s, m);
        assert n % s == 0;
        CoderUtil.resetOutputBuffers(decodingState.outputs,
                decodingState.outputOffsets, dataLen);

        if (decodingState.erasedIndexes.length == 1){
            singlePrepareDecoding(decodingState.inputs, decodingState.erasedIndexes);
            byte[][] realInputs = new byte[(n-1)*l/r][];
            for (int i = 0; i < (n-1)*l/r; i++)
                realInputs[i] = decodingState.inputs[validIndexes[i]];
            MSRDecode(decodeMatrix, dataLen, realInputs, decodingState.outputs);
            // singleMSRDecode();
        } else {
            if (isUsePCM()) {
                mulPrepareAPCMDecoding(decodingState.inputs, decodingState.erasedIndexes);
            } else {
                mulPrepareDecoding(decodingState.inputs, decodingState.erasedIndexes);
            }
            byte[][] realInputs = new byte[k*l][];
            int[] realInputOffsets = new int[getNumDataUnits()*l];
            for (int i = 0; i < k*l; i++) {
                realInputs[i] = decodingState.inputs[validIndexes[i]];
                realInputOffsets[i] = decodingState.inputOffsets[validIndexes[i]];
            }
            this.gfTables = new byte[decodingState.erasedIndexes.length*l * k*l * 32];
            // MSRDecode(decodeMatrix, dataLen, realInputs, decodingState.outputs);
            RSUtil.initTables(getNumDataUnits()*l, decodingState.erasedIndexes.length*l,
                    decodeMatrix, 0, gfTables);
            RSUtil.encodeData(gfTables, dataLen, realInputs, realInputOffsets,
                    decodingState.outputs, decodingState.outputOffsets);
        }
    }

    private void MSRDecode(byte[] encodeMatrix, int encodeLen, byte[][] inputs,
                           byte[][] outputs) {
        int n = getNumAllUnits();
        int k = getNumDataUnits();
        int r = n - k;
        int s = r;
        int m = n/s;
        int l = (int)Math.pow(s, m);
        int numInputs = inputs.length;
        int numOutputs = outputs.length;

        // DumpUtil.dumpMatrix(encodeMatrix, numOutputs, numInputs);
        // the encodeLen must be the mulriple of l and some other assertion
        // start to encode
        int i, j, p;
        byte temp;
        for (i=0; i < numOutputs; i++) {
            for (j=0; j < encodeLen; j++) {
                // calculate the lost data
                temp = 0;
                for (p=0; p < numInputs; p++)
                    temp ^= GF256.gfMul(encodeMatrix[i*numInputs + p], inputs[p][j]);
                outputs[i][j] = temp;
            }
        }
    }

    private void mulPrepareAPCMDecoding(byte[][] inputs, int[] erasedIndexes) {
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

        // find valid input k*l size
        int[] tmpValidIndexes = CoderUtil.getValidIndexes(inputs);
        this.validIndexes = Arrays.copyOf(tmpValidIndexes, tmpValidIndexes.length);
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

    private void mulPreparePCMDecoding(byte[][] inputs, int[] erasedIndexes){
        int n = getNumAllUnits();
        int k = getNumDataUnits();
        int r = n - k;
        int s = r;
        int m = n / s;
        int l = (int)Math.pow(s, m);
        int i, j, p, q, e;
        assert numErasedDataUnits == erasedIndexes.length;

        // find valid input k*l size
        int[] tmpValidIndexes = CoderUtil.getValidIndexes(inputs);
        this.validIndexes = Arrays.copyOf(tmpValidIndexes, tmpValidIndexes.length);
        // generate decode matrix
        decodeMatrix = new byte[numErasedDataUnits*l*k*l];
        byte[] tmpDecodeMatrix = new byte[r*l * k*l];
        byte[] rightDecodeMatrix = new byte[r*l * k*l];
        byte[] leftMatrix = new byte[r*l*r*l];
        byte[] invertLeftMatrix = new byte[r*l*r*l];

        for (i=0; i<r*l; i++) {
            int idx = 0;
            int pdx = 0;
            for (j=0; j<n*l; j++){
                if (validIndexes.length > idx && validIndexes[idx] == j){
                    rightDecodeMatrix[i*k*l + idx] = MSRMatrix[i*n*l + j];
                    idx++;
                } else {
                    leftMatrix[i*r*l + pdx] = MSRMatrix[i*n*l + j];
                    pdx++;
                }
            }
        }
        // DumpUtil.dumpMatrix(leftMatrix, r*l, r*l);

        GF256.gfInvertMatrix(leftMatrix, invertLeftMatrix, r*l);
        byte temp;
        for (i=0; i < r*l; i++)
            for (j=0; j < k*l; j++) {
                temp = 0;
                for (e=0; e < r*l; e++)
                    temp ^= GF256.gfMul(invertLeftMatrix[i*r*l + e], rightDecodeMatrix[e*k*l + j]);
                tmpDecodeMatrix[i*k*l + j] = temp;
            }

        for (i=0; i < numErasedDataUnits*l; i++){
            for (j=0; j < k*l; j++) {
                decodeMatrix[i*k*l + j] = tmpDecodeMatrix[i*k*l + j];
            }
        }
    }

    private void mulPrepareDecoding(byte[][] inputs, int[] erasedIndexes) {
        int n = getNumAllUnits();
        int k = getNumDataUnits();
        int r = n - k;
        int s = r;
        int m = n / s;
        int l = (int)Math.pow(s, m);
        int i, j, p, q, t;
        r = erasedIndexes.length;

        byte[] encodeMatrix = new byte[getNumAllUnits() * getNumDataUnits() * l * l];
        byte[] tmpMatrix = new byte[getNumDataUnits() * getNumDataUnits() * l * l];
        byte[] invertMatrix = new byte[getNumDataUnits() * getNumDataUnits() * l * l];
        decodeMatrix = new byte[r * k * l * l];
        RSUtil.genMSREncodeMatrix(MSRMatrix, encodeMatrix, getNumAllUnits(), getNumDataUnits());

        // DumpUtil.dumpMatrix(MSRMatrix, s*l, n*l);

        this.erasureFlags = new boolean[n*l];
        this.numErasedDataUnits = 0;
        // numErasedDataUnits = decodingState.erasedIndexes.length;

        for (i = 0; i < erasedIndexes.length; i++) {
            int index =erasedIndexes[i];
            for (j=0; j < l; j++) {
                erasureFlags[index * l + j] = true;
                if (index < getNumDataUnits()) {
                    numErasedDataUnits++;
                }
            }
        }
        assert numErasedDataUnits/l == erasedIndexes.length;

        // find valid input k*l size
        int[] tmpValidIndexes = CoderUtil.getValidIndexes(inputs);
        this.validIndexes = Arrays.copyOf(tmpValidIndexes, tmpValidIndexes.length);

        for (i = 0; i < k*l; i++) {
            t = validIndexes[i];
            for (j = 0; j < k*l; j++) {
                tmpMatrix[k*l * i + j] =
                        encodeMatrix[k*l * t + j];
            }
        }

        GF256.gfInvertMatrix(tmpMatrix, invertMatrix, getNumDataUnits()*l);

        for (i = 0; i < numErasedDataUnits/l; i++) {
            for (q = 0; q < l; q++) {
                for (j = 0; j < k*l; j++) {
                    decodeMatrix[k*l * (i*l+q) + j] =
                            invertMatrix[getNumDataUnits()*l * (erasedIndexes[i]*l+q) + j];
                }
            }
        }

        for (p = numErasedDataUnits/l; p < r; p++) {
            for (q = 0; q < l; q++) {
                for (i = 0; i < getNumDataUnits()*l; i++) {
                    byte ttmp = 0;
                    for (j = 0; j < getNumDataUnits()*l; j++) {
                        ttmp ^= GF256.gfMul(invertMatrix[j * getNumDataUnits()*l + i],
                                encodeMatrix[getNumDataUnits()*l * (erasedIndexes[p]*l+q) + j]);
                    }
                    decodeMatrix[getNumDataUnits()*l * (p*l+q) + i] = ttmp;
                }
            }
        }
    }

    private void singlePrepareDecoding(byte[][] inputs, int[] erasedIndexes) {
        int i, j, p, q, e, w;
        int n = getNumAllUnits();
        int k = getNumDataUnits();
        int v, u;
        int a, b, temp;
        int r = n - k;
        int s = r;
        int m = n / s;
        int l = (int)Math.pow(s, m);
        int errorTip = erasedIndexes[0];

        // find valid input k*l size
        int[] tmpValidIndexes = CoderUtil.getValidIndexes(inputs);
        this.validIndexes = Arrays.copyOf(tmpValidIndexes, tmpValidIndexes.length);
        // initialize
        int[] Ary = new int[m];
        int[] a_index = new int[l/s];
        u = errorTip % s;
        v = (errorTip-u)/s + 1;
        byte[] leftMatrix = new byte[l*l];
        byte[] invertleftMatrix = new byte[l*l];
        byte[] tmpMatrix = new byte[l*(n-1)*l/r];
        decodeMatrix = new byte[l*(n-1)*l/r];

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

    public static int[][] searchData(int[] erasedIndexes, int n, int k){
        int i, j, e, p, max;
        int v, u;
        int r = n - k;
        int s = r;
        int m = n / s;
        int l = (int)Math.pow(s, m);
        int[][] dataIndexes = new int[n][];
        for (i=0; i < n; i++)
            dataIndexes[i] = new int[l];
        int flag = 1;
        int errorLen = erasedIndexes.length;
        if (errorLen > 1) {
            // multiple error nodes
            max = 0;
            for (i=0; i < n; i++) {
                if (max < k) {
                    // determine if it is a lost disk
                    flag = 1;
                    for (j=0; j < errorLen; j++)
                        if ( i == erasedIndexes[j])
                            flag = 0;
                    // get msr data index
                    if (flag != 0) {
                        for (j=0; j < l; j++)
                            dataIndexes[i][j] = 1;
                        max++;
                    } else {
                        for (j=0; j < l; j++)
                            dataIndexes[i][j] = 0;
                    }
                } else {
                    for (j=0; j < l; j++)
                        dataIndexes[i][j] = 0;
                }
            }
        } else {
            // single node recovery
            for (i=0; i < n; i++)
                for (j=0; j < n; j++)
                    dataIndexes[i][j] = 0;
            u = erasedIndexes[0] % s;
            v = (erasedIndexes[0]-u)/s + 1;
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
                        if (p != erasedIndexes[0]) dataIndexes[p][i] = 1;
                }
            }
        }
        return dataIndexes;
    }
}




















