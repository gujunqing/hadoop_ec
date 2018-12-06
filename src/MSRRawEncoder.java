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

/**
 * A raw erasure encoder in RS code scheme in pure Java in case native one
 * isn't available in some environment. Please always use native implementations
 * when possible. This new Java coder is about 5X faster than the one originated
 * from HDFS-RAID, and also compatible with the native/ISA-L coder.
 */
public class MSRRawEncoder  extends RawErasureEncoder{
    // relevant to schema and won't change during encode calls.
    private byte[] encodeMatrix;
    private byte[] MSRMatrix;
    /**
     * Array of input tables generated from coding coefficients previously.
     * Must be of size 32*k*rows
     */
    // private byte[] gfTables;

    public MSRRawEncoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);

        if (getNumAllUnits() >= RSUtil.GF.getFieldSize()) {
            System.out.println(
                    "Invalid numDataUnits and numParityUnits");
        }
        // generate the encodeMatrix
        assert getNumAllUnits() % getNumParityUnits() == 0;
        int m = getNumAllUnits() / getNumParityUnits();
        int s = getNumParityUnits();
        int l = (int)Math.pow(s, m);
        encodeMatrix = new byte[getNumAllUnits() * getNumDataUnits() * l * l];
        MSRMatrix = new byte[getNumParityUnits() * getNumAllUnits() * l * l];

        // RSUtil.genCauchyMatrix(encodeMatrix, getNumAllUnits(), getNumDataUnits());
        RSUtil.genMSRMatrix(MSRMatrix, getNumAllUnits(), getNumDataUnits());
        // DumpUtil.dumpMatrix(MSRMatrix, getNumParityUnits()*l, getNumAllUnits()*l);
        RSUtil.genMSREncodeMatrix(MSRMatrix, encodeMatrix, getNumAllUnits(), getNumDataUnits());
        // todo gfTable is not supported right now
        // gfTables = new byte[getNumAllUnits() * getNumDataUnits() * l * l * 32];
        // RSUtil.initTables(getNumDataUnits() * l, getNumParityUnits() * l, encodeMatrix,
        //        getNumDataUnits() * l * getNumDataUnits() * l, gfTables);
    }

    private void MSREncodeData(byte[] encodeMatrix, int encodeLen, byte[][] inputs,
                              byte[][] outputs) {
        int n = getNumAllUnits();
        int k = getNumDataUnits();
        int r = n - k;
        int s = r;
        int m = n/s;
        int l = (int)Math.pow(s, m);
        int numInputs = inputs.length;
        int numOutputs = outputs.length;

        // the encodeLen must be the mulriple of l and some other assertion
        assert numInputs == k*l;
        assert numOutputs == r*l;

        // start to encode
        int i, j, p;
        byte temp;
        for (i=0; i < r*l; i++) {
            for (j=0; j < encodeLen; j++) {
                // calculate the parity
                temp = 0;
                for (p=0; p < k*l; p++)
                    temp ^= GF256.gfMul(encodeMatrix[i*k*l + p], inputs[p][j]);
                outputs[i][j] = temp;
            }
        }
    }

    @Override
    protected void doEncode(ByteArrayEncodingState encodingState) {
        // set output buffer to zero
        CoderUtil.resetOutputBuffers(encodingState.outputs,
                encodingState.outputOffsets,
                encodingState.encodeLength);
        // todo, encode data, not support gftable right now
        MSREncodeData(encodeMatrix, encodingState.encodeLength,
                encodingState.inputs, encodingState.outputs);
    }
}
