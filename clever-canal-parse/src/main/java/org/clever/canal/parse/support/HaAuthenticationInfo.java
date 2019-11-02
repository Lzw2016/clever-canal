package org.clever.canal.parse.support;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@SuppressWarnings("unused")
public class HaAuthenticationInfo {

    @Getter
    @Setter
    private AuthenticationInfo master;
    @Getter
    private List<AuthenticationInfo> slavers = new ArrayList<>();

    public void addSlaver(AuthenticationInfo slaver) {
        this.slavers.add(slaver);
    }

    public void addSlavers(Collection<AuthenticationInfo> slavers) {
        this.slavers.addAll(slavers);
    }
}
