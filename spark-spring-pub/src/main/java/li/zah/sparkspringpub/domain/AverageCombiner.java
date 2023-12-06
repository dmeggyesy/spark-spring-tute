package li.zah.sparkspringpub.domain;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import lombok.Getter;

public class AverageCombiner implements Serializable {

  @Getter
  private BigDecimal sum = BigDecimal.ZERO;

  private long count = 0;

  public void add(long value) {
    sum = sum.add(BigDecimal.valueOf(value));
    count++;
  }

  public BigDecimal average() {
    return sum.divide(BigDecimal.valueOf(count), 2, RoundingMode.HALF_UP);
  }
}
