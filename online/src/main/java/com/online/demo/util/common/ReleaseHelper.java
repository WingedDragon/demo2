package com.online.demo.util.common;

import com.google.common.collect.Maps;
import com.online.demo.enumeration.BannerTypeEnum;
import com.online.demo.enumeration.BiddingStatusEnum;
import com.online.demo.enumeration.DeviceTypeEnum;
import com.online.demo.util.constant.ReleaseConstant;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by finup on 2018/7/16.
 */
public class ReleaseHelper {

    public static final String BIDDING_SUC = "0";//竞价成功
    public static final String BIDDING_FAIL = "1";//竞价失败



    public static final String SOURCE_TOUTIAO = "toutiao";
    public static final String SOURCE_WANGYI = "wangyi";
    public static final String SOURCE_XIMALAYA = "ximalaya";
    public static final String SOURCE_LIEBAO = "liebao";
    public static final String SOURCE_QQ = "qq";

    public static int getRandomNum(int count){
        int num = 0;
        try{
            String range = "123456789";
            int len = range.length();
            StringBuffer sb = new StringBuffer();
            for(int i=0;i<count;i++){
                int idx = new Random().nextInt(10)+1;
                if(idx >= len){
                    sb.append(range.charAt(idx-len));
                }else {
                    sb.append(range.charAt(idx));
                }
            }
            String numstr = sb.toString();
            num = Integer.valueOf(numstr);
        }catch(Exception e){
            e.printStackTrace();
        }

        return num;
    }

    public static Double getRandomDouble(int intLen, int deciLen){
        String range = "0123456789";
        int len = range.length();
        StringBuffer sb = new StringBuffer();
        for(int i=0;i<intLen;i++){
            int idx = new Random().nextInt(10)+1;
            if(idx >= len){
                sb.append(range.charAt(idx-len));
            }else {
                sb.append(range.charAt(idx));
            }
        }

        sb.append(".");
        for(int i=0;i<deciLen;i++){
            int idx = new Random().nextInt(10)+1;
            if(idx >= len){
                sb.append(range.charAt(idx-len));
            }else {
                sb.append(range.charAt(idx));
            }
        }

        String s = sb.toString();

        return Double.valueOf(s);
    }

    public static String getRandomChar(int count){
        String s = "abcdefghijkmnopqrstuvwxz";
        StringBuffer sb = new StringBuffer();
        for(int i=0;i<count;i++){
            int idx = new Random().nextInt(22)+1;
            sb.append(s.charAt(idx));
        }
        return sb.toString();
    }

    public static String getRandomDate(int count){
        String s = "abcdefghijkmnopqrstuvwxz";
        StringBuffer sb = new StringBuffer();
        for(int i=0;i<count;i++){
            int idx = new Random().nextInt(22)+1;
            sb.append(s.charAt(idx));
        }
        return sb.toString();
    }

    public static String getRandomDate(int type, int range, String formatter){
        boolean sign = (new Random().nextInt(range)+1)%2==1;
        int random = new Random().nextInt(range)+1;
        int num = random * (sign?1:-1);
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(type, num);
        return formatDate4Def(cal.getTime(), formatter);
    }

    public static Date getRandomDate(int type, int range){
        boolean sign = (new Random().nextInt(range)+1)%2==1;
        int random = new Random().nextInt(range)+1;
        int num = random * (sign?1:-1);
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(type, num);
        return cal.getTime();
    }


    public static String formatDate4Def(Date date, String formatter) {
        SimpleDateFormat sdf = new SimpleDateFormat(formatter);
        String result = null;
        try {
            if (null != date) {
                result = sdf.format(date);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    //===================================================

    public static String getSource(){
        List<String> sources = Arrays.asList(SOURCE_TOUTIAO, SOURCE_XIMALAYA, SOURCE_WANGYI, SOURCE_LIEBAO, SOURCE_QQ);
        int num = new Random().nextInt(5)+1;
        int idx = num % sources.size();
        return sources.get(idx);
    }

    public static String getStatus(){
        List<String> sources = Arrays.asList(ReleaseConstant.RELEASE_NOTCUSTOMER, ReleaseConstant.RELEASE_CUSTOMER);
        int num = new Random().nextInt(2)+1;
        int idx = num % sources.size();
        return sources.get(idx);
    }

    public static String getBiddingSelfStatus(){
        List<String> status = Arrays.asList(BiddingStatusEnum.SUCCESS.getCode(), BiddingStatusEnum.FAILURE.getCode());
        int num = new Random().nextInt(2)+1;
        int idx = num % status.size();
        return status.get(idx);
    }

    public static String getBiddingStatus(){
        return ReleaseConstant.RELEASE_BIDDING;
    }


    public static String getDeviceType(){
        List<String> deviceTypes = Arrays.asList(DeviceTypeEnum.ANDROID.getCode(), DeviceTypeEnum.IOS.getCode(), DeviceTypeEnum.OTHER.getCode());
        int num = new Random().nextInt(3)+1;
        int idx = num % deviceTypes.size();
        return deviceTypes.get(idx);
    }

    public static String getBannerType(){
        List<String> bannerTypes = Arrays.asList(BannerTypeEnum.QQ_BANNER_1001.getCode(), BannerTypeEnum.QQ_BANNER_0802.getCode());
        int num = new Random().nextInt(3)+1;
        int idx = num % bannerTypes.size();
        return bannerTypes.get(idx);
    }

    public static Map<String,String> getExts(String status){
        Map<String,String> maps = Maps.newHashMap();

        if(ReleaseConstant.RELEASE_NOTCUSTOMER.equalsIgnoreCase(status) ){
            maps.put("bid", String.valueOf(getRandomNum(1)));

            int num = getRandomNum(1) % 2 + 1 ;
            maps.put("code", String.valueOf(num));

        }else if(ReleaseConstant.RELEASE_CUSTOMER.equalsIgnoreCase(status)){

            maps.put("code", "0");

        }else if(ReleaseConstant.RELEASE_BIDDING.equalsIgnoreCase(status)){

            maps.put("price", String.valueOf(getRandomDouble(2, 1)));
            int random =  new Random().nextInt(2)%2;
            String code = BIDDING_FAIL;
            if(random == 0){
                code = BIDDING_SUC;
            }
            maps.put("code", code);

        }
        return maps;
    }

}
