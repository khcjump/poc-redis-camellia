package com.example.sync.common.config;

import com.example.sync.common.model.AppRole;
import com.example.sync.common.model.Location;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 應用層設定（prefix = {@code app}）。
 */
@ConfigurationProperties(prefix = "app")
public class AppProperties {

    /** 運作角色（RedisGateway / SyncWorker / Console / All） */
    private AppRole role = AppRole.All;

    /** 部署位置（Cloud / OnPrem） */
    private Location location = Location.Cloud;

    public AppRole getRole() {
        return role;
    }

    public void setRole(AppRole role) {
        this.role = role;
    }

    public Location getLocation() {
        return location;
    }

    public void setLocation(Location location) {
        this.location = location;
    }
}
