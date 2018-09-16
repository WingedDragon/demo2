package com.online.demo.enumeration;

import org.apache.commons.lang3.StringUtils;

/**
 * 竞价结果
 */
public enum BiddingStatusEnum {
    SUCCESS("1", "获胜"),
    FAILURE("0", "失败");

    BiddingStatusEnum(String code, String desc) {
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

    public static BiddingStatusEnum getByCode(String code) {
        if (StringUtils.isBlank(code)) {
            return null;
        }
        for (BiddingStatusEnum typeEnum : values()) {
            if (typeEnum.getCode().equals(code)) {
                return typeEnum;
            }
        }
        return null;
    }
}
