package com.online.demo.domain;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.online.demo.enumeration.BusyDBEnum;
import com.online.demo.enumeration.PricingTypeEnum;
import com.online.demo.util.common.CommonUtil;
import com.online.demo.util.common.ReleaseHelper;
import com.online.demo.util.constant.ReleaseConstant;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

/**
 * Created by finup on 2018/8/10.
 */
public class ReleaseBidding implements Serializable{


    //请求id
    private String requestId;

    //会话id
    private String sessionId;

    //设备类型
    private String devicenumType;

    //竞价结果
    private String status;

    //价格
    private Long price;

    //广告位类型
    private String bannerType;

    //渠道
    private String source;

    //时间
    private Long ct;

    //时间
    private Long ut;


    //竞价方式（PMP|RTB)
    private String pricingType;

    //竞价类型
    private String biddingType;

    //时间（日）
    private String bdp_day;


    public static ReleaseBidding createRadomReleaseBidding(){
        ReleaseBidding bidding = new ReleaseBidding();

        //时间参数、时间范围、时间格式化
        int dateType = Calendar.DAY_OF_MONTH;
        int dateRange = 10;
        Date ctDate = ReleaseHelper.getRandomDate(dateType, dateRange);
        long ctTime = ctDate.getTime();
        String date = ReleaseHelper.formatDate4Def(ctDate, "yyyyMMddHHmmss");
        String day = ReleaseHelper.formatDate4Def(ctDate, "yyyyMMdd");

        String reqRandoms = ReleaseHelper.getRandomChar(6);
        String sessionRandoms = ReleaseHelper.getRandomChar(6);
        Long price = Long.valueOf(ReleaseHelper.getRandomNum(4));

        //请求id
        String reqId = date + reqRandoms;
        //会话id
        String sessionId = date + sessionRandoms;
        //设备号
        String device = ReleaseHelper.getRandomChar(3);
        //渠道
        String source = ReleaseHelper.getSource();
        //状态
        String status = ReleaseHelper.getBiddingSelfStatus();
        //设备类型
        String deviceType = ReleaseHelper.getDeviceType();
        //竞价方式
        String biddingType = ReleaseHelper.getDeviceType();
        //广告位类型
        String bannerType = ReleaseHelper.getBannerType();
        //计费方式
        String pricingType = PricingTypeEnum.CPM.getCode();


        bidding.setRequestId(reqId);//请求id
        bidding.setSessionId(sessionId);//会话id
        bidding.setDevicenumType(deviceType);////设备类型
        bidding.setStatus(status);//竞价结果
        bidding.setPrice(price);//价格
        bidding.setBiddingType(biddingType);//竞价类型（PMP|RTB)
        bidding.setSource(source);//渠道
        bidding.setCt(ctTime);//时间
        bidding.setUt(ctTime);
        bidding.setPricingType(pricingType);//竞价方式
        bidding.setBannerType(bannerType);//广告位类型
        bidding.setBdp_day(day);

        return bidding;
    }


    /**
     * 创造kafka使用的消息数据
     * @return
     */
    public static Map<String,String> createReleaseBidding4Kafka(){
        Map<String,String> datas = Maps.newHashMap();

        //随机码
        UUID uuid = UUID.randomUUID();

        //竞价信息
        BusyDBEnum biddingEnum = BusyDBEnum.REALEASE_BIDDING;
        String biddingKey = biddingEnum.getDb() + ReleaseConstant.BOTTOM_LINE
                          + biddingEnum.getTable() + ReleaseConstant.BOTTOM_LINE
                          + uuid.toString();

        //json数据
        ReleaseBidding releaseBidding = createRadomReleaseBidding();
        String biddingValue = JSON.toJSONString(releaseBidding);


        datas.put(biddingKey, biddingValue);

        return datas;
    }



    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getDevicenumType() {
        return devicenumType;
    }

    public void setDevicenumType(String devicenumType) {
        this.devicenumType = devicenumType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getPrice() {
        return price;
    }

    public void setPrice(Long price) {
        this.price = price;
    }

    public String getBannerType() {
        return bannerType;
    }

    public void setBannerType(String bannerType) {
        this.bannerType = bannerType;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Long getCt() {
        return ct;
    }

    public void setCt(Long ct) {
        this.ct = ct;
    }

    public String getPricingType() {
        return pricingType;
    }

    public void setPricingType(String pricingType) {
        this.pricingType = pricingType;
    }

    public String getBiddingType() {
        return biddingType;
    }

    public void setBiddingType(String biddingType) {
        this.biddingType = biddingType;
    }

    public String getBdp_day() {
        return bdp_day;
    }

    public void setBdp_day(String bdp_day) {
        this.bdp_day = bdp_day;
    }

    public Long getUt() {
        return ut;
    }

    public void setUt(Long ut) {
        this.ut = ut;
    }

}
