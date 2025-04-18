

import java.math.BigDecimal;
import java.math.BigInteger;

public class PageAccessTracker {
    private static final ThreadLocal<BigInteger> counter = ThreadLocal.withInitial(() -> new BigInteger("0"));
    public static void increment() { counter.set(counter.get().add(new BigInteger("1"))); }
    public static BigInteger getAccessCount() { return counter.get(); }
    public static void reset() { counter.set(new BigInteger("0")); }
}
