package org.clever.canal.parse.inbound.mysql.rds.request;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.clever.canal.parse.inbound.mysql.rds.data.DescribeBinlogFileResult;

import java.util.Date;

@SuppressWarnings("unused")
public class DescribeBinlogFilesRequest extends AbstractRequest<DescribeBinlogFileResult> {

    public DescribeBinlogFilesRequest() {
        setVersion("2014-08-15");
        putQueryString("Action", "DescribeBinlogFiles");
    }

    public void setRdsInstanceId(String rdsInstanceId) {
        putQueryString("DBInstanceId", rdsInstanceId);
    }

    public void setPageSize(int pageSize) {
        putQueryString("PageSize", String.valueOf(pageSize));
    }

    public void setPageNumber(int pageNumber) {
        putQueryString("PageNumber", String.valueOf(pageNumber));
    }

    public void setStartDate(Date startDate) {
        putQueryString("StartTime", formatUTCTZ(startDate));
    }

    public void setEndDate(Date endDate) {
        putQueryString("EndTime", formatUTCTZ(endDate));
    }

    public void setResourceOwnerId(Long resourceOwnerId) {
        putQueryString("ResourceOwnerId", String.valueOf(resourceOwnerId));
    }

    @Override
    protected DescribeBinlogFileResult processResult(HttpResponse response) throws Exception {
        String result = EntityUtils.toString(response.getEntity());
        return JSONObject.parseObject(
                result,
                new TypeReference<DescribeBinlogFileResult>() {
                }
        );
    }
}
