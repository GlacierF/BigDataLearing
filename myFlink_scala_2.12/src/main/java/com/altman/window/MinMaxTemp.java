package com.altman.window;

/**
 * 计算窗口最大最小值的算子需要使用的实体类
 * @author : Altman
 */
public class MinMaxTemp {
    /**
     * 用于保存传感器id
     */
    private String id ;
    /**
     * 用于保存窗口内此传感器的最低温度
     */
    private Double min ;
    /**
     * 用于保存窗口内此传感器的最高温度
     */
    private Double max ;
    /**
     * 用于保存此窗口的结束时间
     */
    private Long endTime ;

    public MinMaxTemp(String id, Double min, Double max, Long endTime) {
        this.id = id;
        this.min = min;
        this.max = max;
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return "MinMaxTemp{" +
                "id='" + id + '\'' +
                ", min=" + min +
                ", max=" + max +
                ", endTime=" + endTime +
                '}';
    }
}
