package com.scanntech.diticketsretropersonal.dto;

public enum MovementStatus {
    PENDING("PENDING"),
    PROCESSED("FINISHED"),
    ERROR("ERROR");

    private final String value;

    MovementStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
