package com.yuandaoma.code.kettleDemo;

import org.pentaho.big.data.kettle.plugins.hdfs.job.JobEntryHadoopCopyFiles;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.special.JobEntrySpecial;
import org.pentaho.di.job.entries.success.JobEntrySuccess;
import org.pentaho.di.job.entry.JobEntryCopy;

public class HadoopCopyFilesJobTest {


    public static void main(String[] args) {

        try {
            KettleEnvironment.init();

            // 创建一个空的作业定义
            JobMeta jobMeta = new JobMeta();
            jobMeta.setName("测试hadoop copy files ");

            // 创建一个开始任务
            JobEntrySpecial start = new JobEntrySpecial();
            start.setName("START");
            start.setStart(true);
            // 作业添加一个节点
            JobEntryCopy startEntry = new JobEntryCopy(start);
            startEntry.setDrawn(true);
            startEntry.setLocation(100,100);
            jobMeta.addJobEntry(startEntry);
            System.out.println("添加Hadoop copy files");



            JobEntryHadoopCopyFiles hadoopCopyFiles = new JobEntryHadoopCopyFiles(null,null,null);
            hadoopCopyFiles.setName("测试Hadoop文件接入");
            // 是否包括子目录
            hadoopCopyFiles.include_subfolders = true;
            // 目标是否是文件
            hadoopCopyFiles.destination_is_a_file = false;
            // 是否复制空目录
            hadoopCopyFiles.copy_empty_folders = true;
            // 是否创建目标目录
            hadoopCopyFiles.create_destination_folder = true;
            // 是否替换已经存在的文件
            hadoopCopyFiles.overwrite_files = false;
            // 是否移除源文件
            hadoopCopyFiles.remove_source_files = false;
            // 是否复制上一个作业结果作为参数
            hadoopCopyFiles.arg_from_previous = false;
            // 增加文件
            hadoopCopyFiles.add_result_filesname = false;
            // 接入源 文件或目录
            hadoopCopyFiles.source_filefolder = new String[] {"pentaho-hdfs://node1:8020/2020-07-23"};
            // 接入目标 文件或目录
            hadoopCopyFiles.destination_filefolder = new String[]{"pentaho-hdfs://node1:8020/hadoop-copy-files-test"};
            // 通配符，要指定，不然会报空指针异常
            hadoopCopyFiles.wildcard = new String[]{""};
            JobEntryCopy hadoopEntry = new JobEntryCopy(hadoopCopyFiles);
            hadoopEntry.setDrawn(true);
            hadoopEntry.setLocation(200,100);
            jobMeta.addJobEntry(hadoopEntry);

            JobHopMeta hadoopHop = new JobHopMeta(startEntry,hadoopEntry);
            hadoopHop.setEnabled(true);
            jobMeta.addJobHop(hadoopHop);

            System.out.println("添加执行成功组件");
            JobEntrySuccess success = new JobEntrySuccess();
            success.setName("Success");
            JobEntryCopy successEntry = new JobEntryCopy(success);
            successEntry.setDrawn(true);
            successEntry.setLocation(300,100);
            jobMeta.addJobEntry(successEntry);

            jobMeta.addJobHop(new JobHopMeta(hadoopEntry,successEntry));

            // 执行任务
            Job job = new Job(null,jobMeta);
            job.setLogLevel(LogLevel.ROWLEVEL);
            job.start();
            job.waitUntilFinished();
            if (job.getErrors() > 0 ){
                System.out.println("任务执行发生异常");
            }

        } catch (KettleException e) {
            e.printStackTrace();
        }

    }
}
