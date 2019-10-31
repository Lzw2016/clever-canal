package org.clever.canal.parse.inbound.mysql.rds.request;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.clever.canal.parse.inbound.mysql.rds.data.RdsBackupPolicy;

/**
 * rds 备份策略查询
 */
@SuppressWarnings("unused")
public class DescribeBackupPolicyRequest extends AbstractRequest<RdsBackupPolicy> {

    public DescribeBackupPolicyRequest() {
        setVersion("2014-08-15");
        putQueryString("Action", "DescribeBackupPolicy");

    }

    public void setRdsInstanceId(String rdsInstanceId) {
        putQueryString("DBInstanceId", rdsInstanceId);
    }

    @Override
    protected RdsBackupPolicy processResult(HttpResponse response) throws Exception {
        String result = EntityUtils.toString(response.getEntity());
        JSONObject jsonObj = JSON.parseObject(result);
        RdsBackupPolicy policy = new RdsBackupPolicy();
        policy.setBackupRetentionPeriod(jsonObj.getString("BackupRetentionPeriod"));
        policy.setBackupLog(jsonObj.getString("BackupLog").equalsIgnoreCase("Enable"));
        policy.setLogBackupRetentionPeriod(jsonObj.getIntValue("LogBackupRetentionPeriod"));
        policy.setPreferredBackupPeriod(jsonObj.getString("PreferredBackupPeriod"));
        policy.setPreferredBackupTime(jsonObj.getString("PreferredBackupTime"));
        return policy;
    }
}
