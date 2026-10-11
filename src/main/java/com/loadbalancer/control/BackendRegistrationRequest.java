package com.loadbalancer.control;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * JSON request body for {@code POST /lb/backends}.
 *
 * <p>Only {@code id}, {@code url}, and {@code weight} are required.
 * {@code maxCapacity} is optional; when absent (or ≤ 0) the registry derives
 * it from {@code weight × K} (where K = {@code lb.capacity-per-weight-unit}).</p>
 *
 * <p>Example payload:
 * <pre>{@code
 * {
 *   "id": "node-1",
 *   "url": "http://localhost:8081",
 *   "weight": 5,
 *   "maxCapacity": 500
 * }
 * }</pre></p>
 */
public class BackendRegistrationRequest {

    @JsonProperty("id")
    private String id;

    @JsonProperty("url")
    private String url;

    @JsonProperty("weight")
    private int weight;

    /**
     * Optional. Omit or set to 0 / negative to let the registry derive the value
     * from {@code weight × K}.
     */
    @JsonProperty("maxCapacity")
    private int maxCapacity = 0;

    // ── Getters & Setters ─────────────────────────────────────────────────

    public String getId()          { return id; }
    public void   setId(String id) { this.id = id; }

    public String getUrl()           { return url; }
    public void   setUrl(String url) { this.url = url; }

    public int  getWeight()          { return weight; }
    public void setWeight(int weight){ this.weight = weight; }

    public int  getMaxCapacity()               { return maxCapacity; }
    public void setMaxCapacity(int maxCapacity) { this.maxCapacity = maxCapacity; }

    /** Basic field-presence validation. Does not check business-logic constraints. */
    public boolean isValid() {
        return id != null && !id.isBlank()
            && url != null && !url.isBlank()
            && weight > 0;
    }

    @Override
    public String toString() {
        return String.format("BackendRegistrationRequest{id='%s', url='%s', weight=%d, maxCapacity=%d}",
                id, url, weight, maxCapacity);
    }
}
