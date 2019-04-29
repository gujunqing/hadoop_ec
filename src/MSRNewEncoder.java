import com.google.common.base.Preconditions;
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

public class MSRNewEncoder  extends RawErasureEncoder{
    // relevant to schema and won't change during encode calls.
    private byte[] encodeMatrix;
    private byte[] MSRMatrix;
    private byte[] gfTables;
    int l;
    final int sliceUnit = 1024;
    /**
     * Array of input tables generated from coding coefficients previously.
     * Must be of size 32*k*rows
     */
    // private byte[] gfTables;
    public MSRNewEncoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);

        if (getNumAllUnits() >= RSUtil.GF.getFieldSize()) {
            System.out.println(
                    "Invalid numDataUnits and numParityUnits");
        }
        if (getNumAllUnits() % getNumParityUnits() != 0) {
            System.out.println(
                    "Invalid numDataUnits and numParityUnits");
        }

        int m = getNumAllUnits() / getNumParityUnits();
        int s = getNumParityUnits();
        l = (int)Math.pow(s, m);
        Preconditions.checkState(sliceUnit%l == 0);
        encodeMatrix = new byte[getNumAllUnits() * getNumDataUnits() * l * l];
        MSRMatrix = new byte[getNumParityUnits() * getNumAllUnits() * l * l];
        RSUtil.genMSRMatrix(MSRMatrix, getNumAllUnits(), getNumDataUnits());
        RSUtil.genMSREncodeMatrix(MSRMatrix, encodeMatrix, getNumAllUnits(), getNumDataUnits());
        if (allowVerboseDump()) {
            DumpUtil.dumpMatrix(encodeMatrix, getNumDataUnits(), getNumAllUnits());
        }
        gfTables = new byte[getNumAllUnits() * getNumDataUnits() * l * l * 32];
        RSUtil.initTables(getNumDataUnits() * l, getNumParityUnits() * l, encodeMatrix,
                getNumDataUnits() * l * getNumDataUnits() * l, gfTables);
        if (allowVerboseDump()) {
            System.out.println(DumpUtil.bytesToHex(gfTables, -1));
        }
    }

    protected void doEncode(ByteBufferEncodingState encodingState) {
        Preconditions.checkState(encodingState.encodeLength%sliceUnit == 0);
        int realEncodeLength = encodingState.encodeLength/l;
        ByteBuffer[] realInput = new ByteBuffer[getNumDataUnits()*l];
        ByteBuffer[] realOutput = new ByteBuffer[getNumParityUnits()*l];
        for (int i=0; i<getNumDataUnits()*l; i++) {
            realInput[i] = ByteBuffer.allocate(realEncodeLength);
        }
        for (int i=0; i<getNumParityUnits()*l; i++) {
            realOutput[i] = ByteBuffer.allocate(realEncodeLength);
        }
        // Change the format of input and output, buffer can be a reference
        int slice = sliceUnit/l;
        for (int i=0; i<getNumDataUnits(); i++) {
            for (int j=0; j<l; j++) {
                for (int k=0; k<encodingState.encodeLength/sliceUnit; k++) {
                    int pos = k*sliceUnit+j*slice;
                    encodingState.inputs[i].limit(pos+slice);
                    encodingState.inputs[i].position(pos);
                    // int realPos = k*slice;
                    realInput[i*l+j].put(encodingState.inputs[i].duplicate());
                }
            }
        }
        for (int i=0; i<getNumDataUnits()*l; i++) {
            realInput[i].flip();
            realInput[i].limit(realEncodeLength);
        }
        //CoderUtil.resetOutputBuffers(encodingState.outputs,
        //        encodingState.encodeLength);
        CoderUtil.resetOutputBuffers(realOutput, realEncodeLength);
        RSUtil.encodeData(gfTables, realInput, realOutput);
        // change the format of output
        for (int i=0; i<getNumParityUnits(); i++) {
            for (int k=0; k<encodingState.encodeLength/sliceUnit; k++) {
                for (int j=0; j<l; j++) {
                    // int pos = k*sliceUnit+j*slice;
                    int realPos = k*slice;
                    realOutput[i*l+j].limit(realPos+slice);
                    realOutput[i*l+j].position(realPos);
                    encodingState.outputs[i].put(realOutput[i*l+j].duplicate());
                }
            }
        }
    }

    @Override
    protected void doEncode(ByteArrayEncodingState encodingState) {
        Preconditions.checkState(encodingState.encodeLength%sliceUnit == 0);
        int realEncodeLength = encodingState.encodeLength/l;
        byte[][] realInput = new byte[getNumDataUnits()*l][];
        byte[][] realOutput = new byte[getNumParityUnits()*l][];
        int[] realInputOffsets = new int[realInput.length];
        int[] realOutputOffsets = new int[realOutput.length];
        for (int i=0; i<getNumDataUnits()*l; i++) {
            realInput[i] = new byte[realEncodeLength];
        }
        for (int i=0; i<getNumParityUnits()*l; i++) {
            realOutput[i] = new byte[realEncodeLength];
        }
        // Change to the real inputs format
        int slice = sliceUnit/l;    //the slice is the smallest unit for decode
        for (int i=0; i<getNumDataUnits(); i++) {
            byte[] tmpInput = encodingState.inputs[i];
            for (int j=0; j<l; j++) {
                for (int k=0; k<encodingState.encodeLength/sliceUnit; k++) {
                    System.arraycopy(tmpInput, k*sliceUnit+j*slice, realInput[i*l+j], k*slice, slice);
                }
            }
        }
        CoderUtil.resetOutputBuffers(encodingState.outputs,
                encodingState.outputOffsets,
                encodingState.encodeLength);
        CoderUtil.resetOutputBuffers(realOutput,
                realOutputOffsets, realEncodeLength);
        RSUtil.encodeData(gfTables, realEncodeLength, realInput,
                realInputOffsets, realOutput, realOutputOffsets);
        // Finally change to real output format
        for (int i=0; i<getNumParityUnits(); i++) {
            for (int k=0; k<encodingState.encodeLength/sliceUnit; k++) {
                for (int j=0; j<l; j++) {
                    System.arraycopy(realOutput[i*l+j], k*slice, encodingState.outputs[i], k*sliceUnit+j*slice, slice);
                }
            }
        }
        /*
        RSUtil.encodeData(gfTables, encodingState.encodeLength,
                encodingState.inputs,
                encodingState.inputOffsets, encodingState.outputs,
                encodingState.outputOffsets);
        */
    }
}
