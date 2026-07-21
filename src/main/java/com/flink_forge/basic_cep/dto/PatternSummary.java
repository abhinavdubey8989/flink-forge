package com.flink_forge.basic_cep.dto;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


/**
 * Represents details of a user who full-filled the pattern:
 *
 *
 */

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PatternSummary {
    private String userId;
    private String patternName;

    // optional
    private Integer cartItemCount;
    private Integer abandonedCartValue;
    private Long lastSeen;
}
