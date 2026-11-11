package com.example.rr.config;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * After the DB endpoint (DNS / TNS alias / service) is repointed to the shadow, pooled connections still talk to
 * the old primary. Soft-evict closes idle connections now and in-use ones when returned; new ones resolve the
 * new endpoint. Secure before use outside test environments.
 */
@RestController
@RequestMapping("/admin/db")
public class DbAdminController {

    private final DataSource dataSource;

    public DbAdminController(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @PostMapping("/evict-pool")
    public Map<String, Object> evictPool() throws SQLException {
        HikariDataSource hikari = dataSource.unwrap(HikariDataSource.class);
        HikariPoolMXBean pool = hikari.getHikariPoolMXBean();
        Map<String, Object> out = new LinkedHashMap<>();
        if (pool == null) {
            out.put("evicted", false);
            out.put("reason", "pool not started");
            return out;
        }
        pool.softEvictConnections();
        out.put("evicted", true);
        out.put("active", pool.getActiveConnections());
        out.put("idle", pool.getIdleConnections());
        return out;
    }
}
