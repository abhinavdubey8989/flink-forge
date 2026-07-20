package com.flink_forge.simple_cep.dto;


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
public class LoginCartInactiveSummary {
    private String userId;
    private int cartItemCount;
    private long lastSeen;
}
