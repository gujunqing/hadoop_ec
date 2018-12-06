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

import java.io.IOException;

/**
 * An abstract raw erasure decoder that's to be inherited by new decoders.
 *
 * Raw erasure coder is part of erasure codec framework, where erasure coder is
 * used to encode/decode a group of blocks (BlockGroup) according to the codec
 * specific BlockGroup layout and logic. An erasure coder extracts chunks of
 * data from the blocks and can employ various low level raw erasure coders to
 * perform encoding/decoding against the chunks.
 *
 * To distinguish from erasure coder, here raw erasure coder is used to mean the
 * low level constructs, since it only takes care of the math calculation with
 * a group of byte buffers.
 *
 * Note it mainly provides decode() calls, which should be stateless and may be
 * made thread-safe in future.
 */
public abstract class RawErasureDecoder {

  private final ErasureCoderOptions coderOptions;

  public RawErasureDecoder(ErasureCoderOptions coderOptions) {
    this.coderOptions = coderOptions;
  }

  /**
   * Decode with inputs and erasedIndexes, generates outputs. More see above.
   *
   * @param inputs input buffers to read data from
   * @param erasedIndexes indexes of erased units in the inputs array
   * @param outputs output buffers to put decoded data into according to
   *                erasedIndexes, ready for read after the call
   * @throws IOException if the decoder is closed.
   */
  public void decode(byte[][] inputs, int[] erasedIndexes, byte[][] outputs)
      throws IOException {
    ByteArrayDecodingState decodingState = new ByteArrayDecodingState(this,
        inputs, erasedIndexes, outputs);

    if (decodingState.decodeLength == 0) {
      return;
    }

    doDecode(decodingState);
  }

  /**
   * Perform the real decoding using bytes array, supporting offsets and
   * lengths.
   * @param decodingState the decoding state
   * @throws IOException if the decoder is closed.
   */
  protected abstract void doDecode(ByteArrayDecodingState decodingState)
      throws IOException;

  public int getNumDataUnits() {
    return coderOptions.getNumDataUnits();
  }

  public int getNumParityUnits() {
    return coderOptions.getNumParityUnits();
  }

  protected int getNumAllUnits() {
    return coderOptions.getNumAllUnits();
  }

  /**
   * Tell if direct buffer is preferred or not. It's for callers to
   * decide how to allocate coding chunk buffers, using DirectByteBuffer or
   * bytes array. It will return false by default.
   * @return true if native buffer is preferred for performance consideration,
   * otherwise false.
   */
  public boolean preferDirectBuffer() {
    return false;
  }

  /**
   * Allow change into input buffers or not while perform encoding/decoding.
   * @return true if it's allowed to change inputs, false otherwise
   */
  public boolean allowChangeInputs() {
    return coderOptions.allowChangeInputs();
  }

  /**
   * Allow to dump verbose info during encoding/decoding.
   * @return true if it's allowed to do verbose dump, false otherwise.
   */
  public boolean allowVerboseDump() {
    return coderOptions.allowVerboseDump();
  }

  /**
   * Should be called when release this coder. Good chance to release encoding
   * or decoding buffers
   */
  public void release() {
    // Nothing to do here.
  }
}
