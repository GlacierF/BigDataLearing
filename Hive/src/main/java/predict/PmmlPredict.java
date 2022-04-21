package predict;

import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.*;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PmmlPredict {
    private Evaluator loadPmml(){
        PMML pmml = new PMML();
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream("E:\\hiveudf\\src\\main\\java\\predict\\tree_version_test.pmml");
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(inputStream == null){
            return null;
        }
        InputStream is = inputStream;
        try {
            pmml = org.jpmml.model.PMMLUtil.unmarshal(is);
        } catch (SAXException e1) {
            e1.printStackTrace();
        } catch (JAXBException e1) {
            e1.printStackTrace();
        }finally {
            //关闭输入流
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
        Evaluator evaluator = modelEvaluatorFactory.newModelEvaluator(pmml);
        pmml = null;
        return evaluator;
    }
    private int predict(Evaluator evaluator,float a, float b, float c, float d) {
//        做预测服务，
//        map<String,Float> data
//        Hbase
//        用户的一个特征库，uid   feature_value   1:3.4,2.5,3.5,3.4,5.6
//        物品的一个特征库，iid   feature_value   2:1,3,10,4
//        实时
//        flink context特征  1.4,2.5
//      data  1:3.4,2.5,3.5,3.4,5.6 （拼接）2:1,3,10,（拼接）1.4,2.5
        Map<String, Float> data = new HashMap<String, Float>();
        data.put("x1", a);
        data.put("x2", b);
        data.put("x3", c);
        data.put("x4", d);

//        X
        List<InputField> inputFields = evaluator.getInputFields();

        Map<FieldName, FieldValue> arguments = new LinkedHashMap<FieldName, FieldValue>();
        for (InputField inputField : inputFields) {
            FieldName inputFieldName = inputField.getName();
            Object rawValue = data.get(inputFieldName.getValue());
            FieldValue inputFieldValue = inputField.prepare(rawValue);
            arguments.put(inputFieldName, inputFieldValue);
        }

        Map<FieldName, ?> results = evaluator.evaluate(arguments);
        List<TargetField> targetFields = evaluator.getTargetFields();

        TargetField targetField = targetFields.get(0);
        FieldName targetFieldName = targetField.getName();

        Object targetFieldValue = results.get(targetFieldName);
        System.out.println("target: " + targetFieldName.getValue() + " value: " + targetFieldValue);
        int primitiveValue = -1;
        if (targetFieldValue instanceof Computable) {
            Computable computable = (Computable) targetFieldValue;
            primitiveValue = (Integer)computable.getResult();
        }
        System.out.println(a + " " + b + " " + c + " " + d + ":" + primitiveValue);
        return primitiveValue;
    }
    public static void main(String args[]){
//        step1: 加载pmml模型实例
        PmmlPredict demo = new PmmlPredict();
//        step2:导入模型
        Evaluator model = demo.loadPmml();
//        step3:预测
        demo.predict(model,4.3f,2.4f,0.9f,0.3f);

    }
}
