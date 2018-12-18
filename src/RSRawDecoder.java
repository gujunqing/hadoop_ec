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
 * A raw erasure decoder in RS code scheme in pure Java in case native one
 * isn't available in some environment. Please always use native implementations
 * when possible. This new Java coder is about 5X faster than the one originated
 * from HDFS-RAID, and also compatible with the native/ISA-L coder.
 */
public class RSRawDecoder extends RawErasureDecoder {
  //relevant to schema and won't change during decode calls
  private byte[] encodeMatrix;

  /**
   * Below are relevant to schema and erased indexes, thus may change during
   * decode calls.
   */
  private byte[] decodeMatrix;
  private byte[] invertMatrix;
  /**
   * Array of input tables generated from coding coefficients previously.
   * Must be of size 32*k*rows
   */
  private byte[] gfTables;
  private int[] cachedErasedIndexes;
  private int[] validIndexes;
  private int numErasedDataUnits;
  private boolean[] erasureFlags;

  public RSRawDecoder(ErasureCoderOptions coderOptions) {
    super(coderOptions);

    int numAllUnits = getNumAllUnits();
    if (getNumAllUnits() >= RSUtil.GF.getFieldSize()) {
      System.out.println(
              "Invalid getNumDataUnits() and numParityUnits");
    }

    encodeMatrix = new byte[numAllUnits * getNumDataUnits()];
    if (isUsePCM())
      RSUtil.genCauchyMatrixPCM(encodeMatrix, numAllUnits, getNumDataUnits());
    else
      RSUtil.genCauchyMatrix(encodeMatrix, numAllUnits, getNumDataUnits());
    if (allowVerboseDump()) {
      DumpUtil.dumpMatrix(encodeMatrix, getNumDataUnits(), numAllUnits);
    }
  }

  @Override
  protected void doDecode(ByteArrayDecodingState decodingState) {
    int dataLen = decodingState.decodeLength;
    CoderUtil.resetOutputBuffers(decodingState.outputs,
        decodingState.outputOffsets, dataLen);
    prepareDecoding(decodingState.inputs, decodingState.erasedIndexes);

    byte[][] realInputs = new byte[getNumDataUnits()][];
    int[] realInputOffsets = new int[getNumDataUnits()];
    for (int i = 0; i < getNumDataUnits(); i++) {
      realInputs[i] = decodingState.inputs[validIndexes[i]];
      realInputOffsets[i] = decodingState.inputOffsets[validIndexes[i]];
    }
    RSUtil.encodeData(gfTables, dataLen, realInputs, realInputOffsets,
        decodingState.outputs, decodingState.outputOffsets);
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
    this.decodeMatrix = new byte[getNumAllUnits() * getNumDataUnits()];
    this.invertMatrix = new byte[getNumAllUnits() * getNumDataUnits()];
    this.gfTables = new byte[getNumAllUnits() * getNumDataUnits() * 32];

    this.erasureFlags = new boolean[getNumAllUnits()];
    this.numErasedDataUnits = 0;

    for (int i = 0; i < erasedIndexes.length; i++) {
      int index = erasedIndexes[i];
      erasureFlags[index] = true;
      if (index < getNumDataUnits()) {
        numErasedDataUnits++;
      }
    }

    if (isUsePCM())
      generatePCMDecodeMatrix(erasedIndexes);
      // generateNewPCMDecodeMatrix(erasedIndexes);
    else
      generateDecodeMatrix(erasedIndexes);

    RSUtil.initTables(getNumDataUnits(), erasedIndexes.length,
        decodeMatrix, 0, gfTables);
    if (allowVerboseDump()) {
      System.out.println(DumpUtil.bytesToHex(gfTables, -1));
    }
  }

  private void generateNewPCMDecodeMatrix(int [] erasedIndexes) {
    int i, j, p, t, pos, idx;
    int k = getNumDataUnits();
    int n = getNumAllUnits();
    int r = erasedIndexes.length;
    byte[] tarIndex = new byte[r*r];
    byte tar, inv, tmp;

    // Construct matrix tmpMatrix from encode matrix
    p = 0;
    for (i = 0; i < r; i++) {
      pos = i*n;
      for (j = 0; j < k; j++) {
        t = validIndexes[j];
        decodeMatrix[k * i + j] = encodeMatrix[pos + t];
      }
      for (j = 0; j < r; j++){
        idx = erasedIndexes[j];
        tarIndex[p++] = encodeMatrix[pos + idx];
      }
    }

    for (i = 0; i < r; i++) {
      tar = tarIndex[i*r+i];
      inv = GF256.gfInv(tar);
      for (j = 0; j < k; j++)
        decodeMatrix[i * k + j] = GF256.gfMul(decodeMatrix[i * k + j], inv);
      for (j=0; j < r; j++)
        tarIndex[i * r + j] = GF256.gfMul(tarIndex[i * r + j], inv);
      for (j = 0; j < r; j++) {
        if (j == i) continue;
        tmp = tarIndex[j*r+i];
        for (p = 0; p < k; p++)
          decodeMatrix[j * k + p] ^= GF256.gfMul(decodeMatrix[i * k + p], tmp);
        for (p = 0; p < r; p++)
          tarIndex[j * r + p] ^= GF256.gfMul(tarIndex[i * r + p], tmp);
      }
    }
    // DumpUtil.dumpMatrix(decodeMatrix, r, k);
  }

  // Generate PCM decode matrix from encode matrix
  private void generatePCMDecodeMatrix(int [] erasedIndexes) {
    int i, j, p;
    // byte[] tmpMatrix = new byte[getNumParityUnits() * getNumAllUnits()];
    int k = getNumDataUnits();
    int n = getNumAllUnits();
    int r = erasedIndexes.length;
    int rr = n-k-r;
    byte tar, inv, tmp;

    // Get invert matrix
    // DumpUtil.dumpMatrix(encodeMatrix, r, n);
    for (i = 0; i < r; i++) {
      int idx = erasedIndexes[i];
      tar = encodeMatrix[i * n + idx];
      if (tar != 1){
        inv = GF256.gfInv(tar);
        for (j = 0; j < n-rr; j++)
          encodeMatrix[i * n + j] = GF256.gfMul(encodeMatrix[i * n + j], inv);
      }
      for (j = 0; j < r; j++) {
        if (j == i)  continue;
        tmp = encodeMatrix[j * n + idx];
        for (p = 0; p < n - rr; p++)
          encodeMatrix[j * n + p] ^= GF256.gfMul(encodeMatrix[i * n + p], tmp);
      }
    }

    // Divide the PCM matrix
    int pos, t;
    for (i = 0; i < r; i++) {
      pos = i*n;
      for (j = 0; j < k; j++) {
        t = validIndexes[j];
        decodeMatrix[k * i + j] = encodeMatrix[pos + t];
      }
    }
  }

  // Generate decode matrix from encode matrix
  private void generateDecodeMatrix(int[] erasedIndexes) {
    int i, j, r, p;
    byte s;
    byte[] tmpMatrix = new byte[getNumAllUnits() * getNumDataUnits()];

    // Construct matrix tmpMatrix by removing error rows
    for (i = 0, r=0; i < getNumDataUnits(); i++) {
      r = validIndexes[i];
      for (j = 0; j < getNumDataUnits(); j++) {
        tmpMatrix[getNumDataUnits() * i + j] =
                encodeMatrix[getNumDataUnits() * r + j];
      }
    }

    GF256.gfInvertMatrix(tmpMatrix, invertMatrix, getNumDataUnits());

    for (i = 0; i < numErasedDataUnits; i++) {
      for (j = 0; j < getNumDataUnits(); j++) {
        decodeMatrix[getNumDataUnits() * i + j] =
                invertMatrix[getNumDataUnits() * erasedIndexes[i] + j];
      }
    }

    for (p = numErasedDataUnits; p < erasedIndexes.length; p++) {
      for (i = 0; i < getNumDataUnits(); i++) {
        s = 0;
        for (j = 0; j < getNumDataUnits(); j++) {
          s ^= GF256.gfMul(invertMatrix[j * getNumDataUnits() + i],
                  encodeMatrix[getNumDataUnits() * erasedIndexes[p] + j]);
        }
        decodeMatrix[getNumDataUnits() * p + i] = s;
      }
    }
  }
}
