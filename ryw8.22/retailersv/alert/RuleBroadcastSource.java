package com.retailersv.alert;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class RuleBroadcastSource extends RichSourceFunction<AlertRule> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<AlertRule> ctx) throws Exception {
        while (running) {
            Connection conn = DriverManager.getConnection("jdbc:mysql://cdh01:3306/flink_wrong", "root", "123456");
            PreparedStatement ps = conn.prepareStatement("SELECT * FROM alert_rule_config WHERE status = 1");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                AlertRule rule = new AlertRule();
                rule.setRule_type(rs.getString("rule_type"));
                rule.setField_name(rs.getString("field_name"));
                rule.setOperator(rs.getString("operator"));
                rule.setThreshold(rs.getDouble("threshold"));
                rule.setDescription(rs.getString("description"));
                rule.setStatus(rs.getInt("status"));
                ctx.collect(rule);
            }
            rs.close();
            ps.close();
            conn.close();

            Thread.sleep(5000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}