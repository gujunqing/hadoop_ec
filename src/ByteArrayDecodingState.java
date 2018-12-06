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


import java.nio.ByteBuffer;

/**
 * A utility class that maintains decoding state during a decode call using
 * byte array inputs.
 */
class ByteArrayDecodingState extends DecodingState {
  byte[][] inputs;
  int[] inputOffsets;
  int[] erasedIndexes;
  byte[][] outputs;
  int[] outputOffsets;

  ByteArrayDecodingState(RawErasureDecoder decoder, byte[][] inputs,
                         int[] erasedIndexes, byte[][] outputs) {
    this.decoder = decoder;
    this.inputs = inputs;
    this.outputs = outputs;
    this.erasedIndexes = erasedIndexes;
    byte[] validInput = CoderUtil.findFirstValidInput(inputs);
    this.decodeLength = validInput.length;

    // checkParameters(inputs, erasedIndexes, outputs);
    // checkInputBuffers(inputs);
    // checkOutputBuffers(outputs);

    this.inputOffsets = new int[inputs.length]; // ALL ZERO
    this.outputOffsets = new int[outputs.length]; // ALL ZERO
  }

  ByteArrayDecodingState(RawErasureDecoder decoder,
                         int decodeLength,
                         int[] erasedIndexes,
                         byte[][] inputs,
                         int[] inputOffsets,
                         byte[][] outputs,
                         int[] outputOffsets) {
    this.decoder = decoder;
    this.decodeLength = decodeLength;
    this.erasedIndexes = erasedIndexes;
    this.inputs = inputs;
    this.outputs = outputs;
    this.inputOffsets = inputOffsets;
    this.outputOffsets = outputOffsets;
  }


  /**
   * Check and ensure the buffers are of the desired length.
   * @param buffers the buffers to check
   */
  void checkInputBuffers(byte[][] buffers) {
    int validInputs = 0;

    for (byte[] buffer : buffers) {
      if (buffer == null) {
        continue;
      }

      if (buffer.length != decodeLength) {
        System.out.println(
            "Invalid buffer, not of length " + decodeLength);
      }

      validInputs++;
    }

    if (validInputs < decoder.getNumDataUnits()) {
      System.out.println(
          "No enough valid inputs are provided, not recoverable");
    }
  }

  /**
   * Check and ensure the buffers are of the desired length.
   * @param buffers the buffers to check
   */
  void checkOutputBuffers(byte[][] buffers) {
    for (byte[] buffer : buffers) {
      if (buffer == null) {
        System.out.println(
            "Invalid buffer found, not allowing null");
      }

      if (buffer.length != decodeLength) {
        System.out.println(
            "Invalid buffer not of length " + decodeLength);
      }
    }
  }
}