package com.dw.demo.enumeration;

import org.apache.commons.lang3.StringUtils;


public enum NationEnum {
    BEI_JING("11", "北京"),
    SHANG_HAI("31", "上海"),
    TIAN_JIN("12", "天津"),
    HE_BEI("13", "河北"),
    SHAN_XI("14", "山西"),
    LIAO_NING("22", "辽宁"),
    SHAN_DONG("37", "山东");


    NationEnum(String code, String desc) {
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

    public static NationEnum getByCode(String code) {
        if (StringUtils.isBlank(code)) {
            return null;
        }
        for (NationEnum typeEnum : values()) {
            if (typeEnum.getCode().equals(code)) {
                return typeEnum;
            }
        }
        return null;
    }
}
