package io.pravega.auth;

import java.io.Serializable;
import java.security.Principal;
import lombok.Data;

@Data
public class MockPrincipal implements Principal, Serializable {
    private static final long serialVersionUID = 1L;
    private final String name;
}
