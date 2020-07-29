package com.yuandaoma.code.kettleDemo;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.gui.Point;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.dummytrans.DummyTransMeta;

import java.util.List;

public class DataDetectionSequenceTest {

    public static void main(String[] args) throws Exception {
        KettleEnvironment.init();

        TransMeta transMeta = new TransMeta("T:\\测试Kettle\\table-sql.ktr");

        List<TransHopMeta> hops = transMeta.getTransHops();
        for (TransHopMeta hop : hops) {
            StepMeta fromStep = hop.getFromStep();
            StepMeta toStep = hop.getToStep();
            System.out.println("fromStep = " + fromStep.getName());
            System.out.println("toStep = " + toStep.getName());
        }


        TransHopMeta transHop = transMeta.getTransHop(transMeta.getTransHops().size() - 1);
        StepMeta toStep = transHop.getToStep();
        System.out.println("hop: " + transHop.getFromStep().getName() + " --> " + toStep.getName());

        DummyTransMeta dummyTransMeta = new DummyTransMeta();


        PluginRegistry registry = PluginRegistry.getInstance();
        StepMeta dummyStepMeta = createStepMeta("空操作", dummyTransMeta, registry);
        dummyStepMeta.setName("空操作（最后一步）");
        Point location = toStep.getLocation();
        dummyStepMeta.setLocation(new Point(location.x + 10, location.y + 10));
        transMeta.addStep(dummyStepMeta);
        transMeta.addTransHop(new TransHopMeta(toStep, dummyStepMeta));


        transHop = transMeta.getTransHop(transMeta.getTransHops().size() - 1);
        System.out.println("hop: " + transHop.getFromStep().getName() + " --> " + toStep.getName());

        String xml = transMeta.getXML();
        System.out.println("xml = " + xml);
        Trans trans = new Trans(transMeta);
        trans.execute(null);
        trans.waitUntilFinished();
        //总共条数
        String total = transMeta.getVariable("total");
        System.out.println("total = " + total);
//        List<StepMeta> transMetaSteps = transMeta.getSteps();
//        StepMeta stepMeta1 = transMetaSteps.get(0);
//        List<TransHopMeta> transHops = transMeta.getTransHops();
//        ExcelOutputMeta excelOutputMeta = new ExcelOutputMeta();
//        DummyTransMeta dummyTransMeta = new DummyTransMeta();
//        PluginRegistry registry = PluginRegistry.getInstance();
//        StepMeta dummyStepMeta = createStepMeta(dummyTransMeta, registry);
//
//        TransHopMeta transHopMeta = new TransHopMeta(dummyStepMeta, dummyStepMeta);
//        transHops.add(transHopMeta);
//        // 执行krt文件
//        Trans trans = new Trans(transMeta);
//        trans.execute(null);
//        // 等待转换执行结束
//        trans.waitUntilFinished();
//
//        // 抛出异常
//        if (trans.getErrors() > 0) {
//            throw new KettleException("There are errors during transformation exception!(传输过程中发生异常)");
//        }
//        List<StepMetaDataCombi> steps = trans.getSteps();
//
//        for (StepMetaDataCombi step : steps) {
//            StepMeta stepMeta = step.stepMeta;
//            //.contains("数据检验")
//            if (stepMeta.getStepID().equals("Validator")) {
//                System.out.println("====================================================");
//                System.out.println("stepMeta.getName() = " + stepMeta.getName());
////                ValidatorMeta validator = (ValidatorMeta) stepMeta.getStepMetaInterface();
////                System.out.println("validator.getValidations() = " + validator.getValidations());
//                StepInterface si = step.step;
//                System.out.println("si.getLinesInput() = " + si.getLinesInput());
//                System.out.println("si.getLinesOutput() = " + si.getLinesOutput());
//                System.out.println("si.getLinesRead() = " + si.getLinesRead());
//                System.out.println("si.getLinesWritten() = " + si.getLinesWritten());
//                System.out.println("si.getLinesRejected() = " + si.getLinesRejected());
//                System.out.println("====================================================");
//            }
////            System.out.println("stepMeta.getXML() = " + stepMeta.getXML());
//        }
//        Result result = trans.getResult();
//        System.out.println("result = " + result.getNrErrors());

//        return trans.getResult();

//        JobMeta jobMeta = new JobMeta("D:\\Programming\\test.kjb", null);
//        Job job = new Job(null, jobMeta);
//        job.setLogLevel(LogLevel.ROWLEVEL);
//        job.start();
//        job.waitUntilFinished();
    }


    public static StepMeta createStepMeta(String stepMame, StepMetaInterface baseStepMeta, PluginRegistry registry) {
        String metaStepId = registry.getPluginId(baseStepMeta);
        String stepName = stepMame;
        StepMeta stepMeta = new StepMeta(metaStepId, stepName, baseStepMeta);
        return stepMeta;
    }
}
