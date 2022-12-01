package polardb

import (
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/polardb"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"net/http"

	"github.com/fengxsong/aliyun-exporter/pkg/client/service"
)

// constants
const (
	name     = "polardb"
	pageSize = 100
)

// Client wrap client
type Client struct {
	*polardb.Client
	desc   *prometheus.Desc
	logger log.Logger
}

// New create ServiceCollector
func New(ak, secret, region string, rt http.RoundTripper, logger log.Logger) (service.Collector, error) {
	client, err := polardb.NewClientWithAccessKey(region, ak, secret)
	if err != nil {
		return nil, err
	}
	client.SetTransport(rt)
	return &Client{Client: client, logger: logger}, nil
}

// Collect collect metrics
func (c *Client) Collect(namespace string, ch chan<- prometheus.Metric) {
	if c.desc == nil {
		c.desc = service.NewInstanceClientDesc(namespace, name, []string{"regionId", "dbInstanceId", "dbType", "desc", "status"})
	}
	req := polardb.CreateDescribeDBClustersRequest()
	req.PageNumber = requests.NewInteger(pageSize)
	instanceCh := make(chan polardb.DBCluster, 1<<10)
	go func() {
		defer close(instanceCh)
		for hasNextPage, pageNum := true, 1; hasNextPage != false; pageNum++ {
			req.PageNumber = requests.NewInteger(pageNum)
			response, err := c.DescribeDBClusters(req)
			if err != nil {
				return
			}
			if len(response.Items.DBCluster) < pageSize {
				hasNextPage = false
			}
			for i := range response.Items.DBCluster {
				instanceCh <- response.Items.DBCluster[i]
			}
		}
	}()

	for ins := range instanceCh {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, 1.0,
			ins.RegionId, ins.DBClusterId, ins.DBClusterNetworkType, ins.DBClusterDescription, ins.DBClusterStatus)
	}
}

func init() {
	service.Register(name, New)
}
