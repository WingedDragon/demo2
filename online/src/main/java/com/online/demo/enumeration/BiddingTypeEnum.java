package com.online.demo.enumeration;

import org.apache.commons.lang3.StringUtils;

/**
 * 竞价方式
 */
public enum BiddingTypeEnum {
    PMP("PMP", "定价"),
    RTB("RTB", "竞价");

    BiddingTypeEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }


    private String code;

    private String lowerCode;

    private String desc;

    public String getCode() {
        return code;
    }


    public String getDesc() {
        return desc;
    }

    public static BiddingTypeEnum getByCode(String code) {
        if (StringUtils.isBlank(code)) {
            return null;
        }
        for (BiddingTypeEnum typeEnum : values()) {
            if (typeEnum.getCode().equals(code)) {
                return typeEnum;
            }
        }
        return null;
    }
}
