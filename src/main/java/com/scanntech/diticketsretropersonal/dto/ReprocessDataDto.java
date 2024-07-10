package com.scanntech.diticketsretropersonal.dto;

import com.fasterxml.jackson.annotation.JsonCreator;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;

public class ReprocessDataDto {
    @Min(1)
    @Max(12)
    private final Integer month;
    @Positive
    private final Integer year;
    private final Integer company;
    private final Integer store;
    @NotNull
    private final MovementStatus status;

    @JsonCreator
    public ReprocessDataDto(Integer month, Integer year, Integer company, Integer store, MovementStatus status) {
        this.month = month;
        this.year = year;
        this.company = company;
        this.store = store;
        this.status = status;
    }

    public String getMonth() {
        return month != null ? String.valueOf(month) : "*";
    }

    public String getYear() {
        return year != null ? String.valueOf(year) : "*";
    }

    public String getCompany() {
        return company != null ? String.valueOf(company) : "*";
    }

    public String getStore() {
        return store != null ? String.valueOf(store): "*";
    }

    public MovementStatus getStatus() {
        return status;
    }

    public String toRegexString() {
        return "%s-%s/%s/%s/%s\\.avro".formatted(getMonth(), getYear(), getCompany(), getStatus().getValue(), getStore());
    }
}
