import static java.lang.System.currentTimeMillis;

public class RequestIdGenerator {

  private volatile long p0, p1, p2, p3, p4, p5, p6, p7;
  private volatile long q0, q1, q2, q3, q4, q5, q6, q7;

  private long counter;

  private volatile long r0, r1, r2, r3, r4, r5, r6, r7;
  private volatile long s0, s1, s2, s3, s4, s5, s6, s7;

  public RequestId generate() {
    counter++;
    return new RequestId(counter, currentTimeMillis());
  }
}
