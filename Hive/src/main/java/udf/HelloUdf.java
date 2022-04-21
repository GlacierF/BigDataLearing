package udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class HelloUdf extends UDF {
    public String evaluate(String input){
    return input.toLowerCase();//将大写字母转换成小写
}
    public int evaluate(int a,int b){
        return a+b;//计算两个数之和
    }

}
