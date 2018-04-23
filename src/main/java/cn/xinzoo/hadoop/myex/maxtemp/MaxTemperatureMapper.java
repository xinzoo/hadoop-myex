package cn.xinzoo.hadoop.myex.maxtemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    /**
     *
     * year:16开始取4位
     * temp:带符号的88位开始取5位 9999为缺省默认值
     * quality质量：93开始取1位
     */
    @Override
    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(15,19);
        String quality = line.substring(92,93);
        int temp ;
        if (line.charAt(87)== '+'){
            temp = Integer.parseInt(line.substring(88,92));
        }else {
            temp = Integer.parseInt(line.substring(87,92));
        }
        if (temp != MISSING && quality.matches("[01459]")){
            Text yk = new Text();
            yk.set(year);
            context.write(yk,new IntWritable(temp));
        }
    }
}
