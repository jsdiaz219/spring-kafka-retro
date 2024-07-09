package com.scanntech.diticketsretropersonal.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;

public class ReprocessDataDto {
    @Min(1)
    @Max(12)
    private final Integer month;
    @Positive
    private final Integer year;
    private final Integer company;
    private final Integer store;

    @JsonCreator
    public ReprocessDataDto(Integer month, Integer year, Integer company, Integer store) {
        this.month = month;
        this.year = year;
        this.company = company;
        this.store = store;
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

    public String toRegexString(MovementStatus status) {
        return "%s-%s/%s/%s/%s\\.avro".formatted(getMonth(), getYear(), getCompany(), status.getValue(), getStore());
    }
}
