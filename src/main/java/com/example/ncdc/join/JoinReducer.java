package com.example.ncdc.join;

import com.example.avro.NcdcRecord;
import com.example.ncdc.NcdcRecordDto;
import com.example.ncdc.StationInfoDto;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<JoinKey, Text, NullWritable, NcdcRecord> {

    /**
     * joinKey<010000-99999,0> station记录A
     * joinKey<010000-99999,1> data记录A
     * joinKey<010000-99999,1> data记录B
     * 这样的数据会归到同一个分区中给到同一个reducer
     * 因为给到reducer时已经根据secondKey排序了，所以第一个就是station的记录
     * 取出station记录，join上data的数据append输出结果到文件
     */
    @Override
    protected void reduce(JoinKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //解析首个行记录station
        Iterator<Text> iter = values.iterator();
        Text stationInfoLine = iter.next();
        StationInfoDto stationInfoDto = parseStationInfoDto(stationInfoLine.toString());

        //后面的都是data记录
        while (iter.hasNext()) {
            Text recordLine =  iter.next();
            NcdcRecordDto dto = parseNcdcRecordDto(recordLine.toString());//解析Data记录

            //下面的过程是拼接两个对象转为一个Avro对象
            NcdcRecord record = new NcdcRecord();
            record.setStationId(key.getFirstKey().toString());
            record.setStationName(stationInfoDto.getStationName());
            record.setStationCity(stationInfoDto.getCity());
            record.setStationState(stationInfoDto.getState());
            record.setStationICAO(stationInfoDto.getICAO());
            record.setStationLatitude(stationInfoDto.getLatitude());
            record.setStationLongitude(stationInfoDto.getLongitude());
            record.setStationElev(stationInfoDto.getElev());
            record.setStationBeginTime(stationInfoDto.getBeginTime());
            record.setStationEndTime(stationInfoDto.getEndTime());
            record.setYear(dto.getYear());
            record.setMonth(dto.getMonth());
            record.setDay(dto.getDay());
            record.setMeanTemp(dto.getMeanTemp());
            record.setMeanTempCount(dto.getMeanTempCount());
            record.setMeanDewPointTemp(dto.getMeanDewPointTemp());
            record.setMeanDewPointTempCount(dto.getMeanDewPointTempCount());
            record.setMeanWindSpeed(dto.getMeanWindSpeed());
            record.setMeanWindSpeedCount(dto.getMeanWindSpeedCount());
            record.setMeanVisibility(dto.getMeanVisibility());
            record.setMeanVisibilityCount(dto.getMeanVisibilityCount());
            record.setMeanStationPressure(dto.getMeanStationPressure());
            record.setMeanStationPressureCount(dto.getMeanStationPressureCount());
            record.setMaxTemp(dto.getMaxTemp());
            record.setMaxTempFlag(dto.getMaxTempFlag());
            record.setMaxGustWindSpeed(dto.getMaxGustWindSpeed());
            record.setMaxSustainedWindSpeed(dto.getMaxSustainedWindSpeed());
            record.setMinTemp(dto.getMinTemp());
            record.setMinTempFlag(dto.getMinTempFlag());
            record.setTotalPrecipitation(dto.getTotalPrecipitation());
            record.setTotalPrecipitationFlag(dto.getTotalPrecipitationFlag());
            record.setSnowDepth(dto.getSnowDepth());
            record.setHasFog(dto.isHasFog());
            record.setHasRain(dto.isHasRain());
            record.setHasHail(dto.isHasHail());
            record.setHasTornado(dto.isHasTornado());
            record.setHasThunder(dto.isHasThunder());
            record.setHasSnow(dto.isHasSnow());
            context.write(null, record);
        }
    }

    private StationInfoDto parseStationInfoDto(String line){
        String[] values = line.split(",");
        StationInfoDto stationInfoDto = new StationInfoDto();
        stationInfoDto.setStationId(values[0].replace("\"", "") + "-" + values[1].replace("\"", ""));
        stationInfoDto.setStationName(values[2].replace("\"", ""));
        stationInfoDto.setCity(values[3].replace("\"", ""));
        stationInfoDto.setState(values[4].replace("\"", ""));
        stationInfoDto.setICAO(values[5].replace("\"", ""));
        stationInfoDto.setLatitude(values[6].replace("\"", ""));
        stationInfoDto.setLongitude(values[7].replace("\"", ""));
        stationInfoDto.setElev(values[8].replace("\"", ""));
        stationInfoDto.setBeginTime(values[9].replace("\"", ""));
        stationInfoDto.setEndTime(values[10].replace("\"", ""));
        return stationInfoDto;
    }

    private NcdcRecordDto parseNcdcRecordDto(String line){
        NcdcRecordDto ncdcRecordDto = new NcdcRecordDto();
        ncdcRecordDto.setStationId(line.substring(0, 6) + "-" + line.substring(7, 12));
        ncdcRecordDto.setYear(line.substring(14, 18));
        ncdcRecordDto.setMonth(line.substring(14, 20));
        ncdcRecordDto.setDay(line.substring(14, 22));
        ncdcRecordDto.setMeanTemp(Double.parseDouble(line.substring(24, 30).trim()));
        ncdcRecordDto.setMeanTempCount(Integer.parseInt(line.substring(31, 33).trim()));
        ncdcRecordDto.setMeanDewPointTemp(Double.parseDouble(line.substring(35, 41).trim()));
        ncdcRecordDto.setMeanDewPointTempCount(Integer.parseInt(line.substring(42, 44).trim()));
        ncdcRecordDto.setMeanSeaLevelPressure(Double.parseDouble(line.substring(46, 52).trim()));
        ncdcRecordDto.setMeanSeaLevelPressureCount(Integer.parseInt(line.substring(53, 55).trim()));
        ncdcRecordDto.setMeanStationPressure(Double.parseDouble(line.substring(57, 63).trim()));
        ncdcRecordDto.setMeanStationPressureCount(Integer.parseInt(line.substring(64, 66).trim()));
        ncdcRecordDto.setMeanVisibility(Double.parseDouble(line.substring(68, 73).trim()));
        ncdcRecordDto.setMeanVisibilityCount(Integer.parseInt(line.substring(74, 76).trim()));
        ncdcRecordDto.setMeanWindSpeed(Double.parseDouble(line.substring(78, 83).trim()));
        ncdcRecordDto.setMeanWindSpeedCount(Integer.parseInt(line.substring(84, 86).trim()));
        ncdcRecordDto.setMaxSustainedWindSpeed(Double.parseDouble(line.substring(88, 93).trim()));
        ncdcRecordDto.setMaxGustWindSpeed(Double.parseDouble(line.substring(95, 100).trim()));
        ncdcRecordDto.setMaxTemp(Double.parseDouble(line.substring(102, 108).trim()));
        ncdcRecordDto.setMaxTempFlag(line.substring(108, 109).trim());
        ncdcRecordDto.setMinTemp(Double.parseDouble(line.substring(110, 116).trim()));
        ncdcRecordDto.setMinTempFlag(line.substring(116, 117));
        ncdcRecordDto.setTotalPrecipitation(Double.parseDouble(line.substring(118, 123).trim()));
        ncdcRecordDto.setTotalPrecipitationFlag(line.substring(123, 124));
        ncdcRecordDto.setSnowDepth(Double.parseDouble(line.substring(125, 130).trim()));

        char[] indicators = line.substring(132, 138).toCharArray();
        ncdcRecordDto.setHasFog(fromDigit(indicators[0]));
        ncdcRecordDto.setHasRain(fromDigit(indicators[1]));
        ncdcRecordDto.setHasSnow(fromDigit(indicators[2]));
        ncdcRecordDto.setHasHail(fromDigit(indicators[3]));
        ncdcRecordDto.setHasThunder(fromDigit(indicators[4]));
        ncdcRecordDto.setHasTornado(fromDigit(indicators[5]));
        return ncdcRecordDto;
    }

    private static boolean fromDigit(char digit) {
        if ('0' == digit) {
            return false;
        }
        return true;
    }
}
