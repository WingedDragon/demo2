package com.online.demo.enumeration;

import org.apache.commons.lang3.StringUtils;

/**
 * 设备类型
 */
public enum DeviceTypeEnum {
    ANDROID("android", "安卓"),
    IOS("ios", "苹果"),
    OTHER("other", "其他");

    DeviceTypeEnum(String code, String desc) {
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

    public static DeviceTypeEnum getByCode(String code) {
        if (StringUtils.isBlank(code)) {
            return null;
        }
        for (DeviceTypeEnum typeEnum : values()) {
            if (typeEnum.getCode().equals(code)) {
                return typeEnum;
            }
        }
        return null;
    }
}
