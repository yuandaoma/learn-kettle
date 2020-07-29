package com.yuandaoma.code.kettleDemo;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaDataCombi;

import java.util.List;

public class KettleTest {

    public static void main(String[] args) throws Exception {
        KettleEnvironment.init();

        TransMeta transMeta = new TransMeta("T:\\测试Kettle\\table-sql.ktr");
        // 执行krt文件
        Trans trans = new Trans(transMeta);
        trans.execute(null);
        // 等待转换执行结束
        trans.waitUntilFinished();

        // 抛出异常
        if (trans.getErrors() > 0) {
            throw new KettleException("There are errors during transformation exception!(传输过程中发生异常)");
        }
        List<StepMetaDataCombi> steps = trans.getSteps();
        for (StepMetaDataCombi step : steps) {

            StepMeta stepMeta = step.stepMeta;
            //.contains("数据检验")
            if (stepMeta.getStepID().equals("Validator")) {
                System.out.println("====================================================");
                System.out.println("stepMeta.getName() = " + stepMeta.getName());
//                ValidatorMeta validator = (ValidatorMeta) stepMeta.getStepMetaInterface();
//                System.out.println("validator.getValidations() = " + validator.getValidations());
                StepInterface si = step.step;
                System.out.println("si.getLinesInput() = " + si.getLinesInput());
                System.out.println("si.getLinesOutput() = " + si.getLinesOutput());
                System.out.println("si.getLinesRead() = " + si.getLinesRead());
                System.out.println("si.getLinesWritten() = " + si.getLinesWritten());
                System.out.println("si.getLinesRejected() = " + si.getLinesRejected());
                System.out.println("====================================================");
            }
//            System.out.println("stepMeta.getXML() = " + stepMeta.getXML());
        }
        Result result = trans.getResult();
        System.out.println("result = " + result.getNrErrors());

//        return trans.getResult();

//        JobMeta jobMeta = new JobMeta("D:\\Programming\\test.kjb", null);
//        Job job = new Job(null, jobMeta);
//        job.setLogLevel(LogLevel.ROWLEVEL);
//        job.start();
//        job.waitUntilFinished();
    }
}
