import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class bufferTest {
    public static void main(String[] args) throws Exception {
        // testCase ec = new testCase(n, k, cellSize);
        ByteBuffer realInput = ByteBuffer.allocate(10);
        ByteBuffer realOutput = ByteBuffer.allocate(10);
        for (int i = 0; i < 10; i++) {
            realInput.put((byte) i);
        }
        realOutput = realInput.duplicate();
        realOutput.position(2);
        realOutput.put((byte)12);
        for (int i = 0; i < 10; i++) {
            System.out.print(realOutput.get(i));
        }
        System.out.println();
        for (int i = 0; i < 10; i++) {
            System.out.print(realInput.get(i));
        }
        /*
        for (int i = 0; i < 10 / 2; i++) {
            int pos = (4 - i) * 2;
            realInput.position(pos);
            realInput.limit(pos + 2);
            realOutput.put(realInput.duplicate());
        }
        realOutput.position(9);
        realOutput.put((byte)12);
        realInput.position(0);
        realInput.limit(10);
        for (int i = 0; i < 10; i++) {
            System.out.print(realOutput.get(i));
        }
        System.out.println();
        for (int i = 0; i < 10; i++) {
            System.out.print(realInput.get(i));
        }
        */
    }
}
