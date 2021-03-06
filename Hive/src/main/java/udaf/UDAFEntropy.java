package udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;


/**
 * Compute the normalized entropy of a series of counts.  The input is assumed
 * to be a column of counts of each value's occurence (if the column contains
 * draws from a probability distribution rather than the distribution itself,
 * consider using FB_MAP_ENTROPY).  The entropy is normalized in the sense that
 * the counts are first normalized so that the sum of all counts equals one
 * (i.e., it is converted to a probability distribution).  Any NULLs in the
 * column are ignored.  If the column contains negative values then NULL is
 * returned.  If the total count of entries of in the column is zero then zero
 * is returned.
 *
 * Note that the entropy is computed using base-2 log.
 */
@Description(name = "entropy",             
             value = "_FUNC_(counts) - Return the normalized entropy of the counts.")
public final class UDAFEntropy extends UDAF {
  /**
   * Implementation note: this is implemented efficiently in one pass.  
   *
   * Let S = sum(x)
   * 
   * -H = sum((x / S) * log(x / S)) 
   *    = (1 / S) sum(x * (log(x) - log(S)))
   *    = (1 / S) (sum(x * log(x)) - log(S) * sum(x))
   *    = (1 / S) sum(x * log(x)) - log(S)
   */
  public static class UDAFEntropyState {
    private double sum_x;
    private double sum_x_log_x;
    private boolean poisoned;
  }

  public static class UDAFEntropyEvaluator implements UDAFEvaluator {
    //声明一个成员变量 state 用来存放一个组的数据
    UDAFEntropyState state;
    //初始化方法，实现
    public UDAFEntropyEvaluator() {
      super();
      state = new UDAFEntropyState();
      init();
    }
    // 确定各个阶段输入输出参数的数据格式ObjectInspectors
    public void init() {
      state.sum_x = 0.0;
      state.sum_x_log_x = 0.0;
      state.poisoned = false;
    }

    private static final double log2 = Math.log(2);

    // map阶段，迭代处理输入sql传过来的列数据
    public boolean iterate(Double x) {
      if (x != null && !state.poisoned) {
        if (x > 0) {
          state.sum_x += x;
          state.sum_x_log_x += x * Math.log(x);
        } else if (x == 0) {
          // Use this slightly convoluted test to ensure that we poison NaNs.
        } else {
          state.poisoned = true;
        }
      }
      return true;
    }

    // map与combiner结束返回结果，得到部分数据聚集结果
    public UDAFEntropyState terminatePartial() {
      return state;
    }
    // combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果。
    public boolean merge(UDAFEntropyState o) {
      state.poisoned |= o.poisoned;
      state.sum_x += o.sum_x;
      state.sum_x_log_x += o.sum_x_log_x;
      return true;
    }
    // reducer阶段，输出最终结果
    public Double terminate() {
      if (state.poisoned) {
        return null;
      }
      if (state.sum_x == 0) {
        return Double.valueOf(0);
      }
      double entropy = -state.sum_x_log_x / state.sum_x + Math.log(state.sum_x);
      // Clip small negative values.
      if (entropy < 0) {
        entropy = 0;
      } else {
        entropy /= log2;
      }

      return Double.valueOf(entropy);
    }
  }
}
