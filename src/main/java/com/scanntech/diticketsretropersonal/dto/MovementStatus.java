package com.scanntech.diticketsretropersonal.dto;

public enum MovementStatus {
    PENDING("pendiente"),
    PROCESSED("finalizado"),
    ERROR("error");

    private final String value;

    private MovementStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
