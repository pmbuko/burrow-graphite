package burrow_graphite

import (
	"context"

	// "fmt"
	"sync"
	"time"
	"strconv"

  log "github.com/Sirupsen/logrus"
	"github.com/marpaia/graphite-golang"
)

type BurrowExporter struct {
	client            *BurrowClient
	graphiteHost      string
	graphitePort      int
	interval          int
	wg                sync.WaitGroup
}

func (be *BurrowExporter) processGroup(cluster, group string, gh *graphite.Graphite) {
	status, err := be.client.ConsumerGroupLag(cluster, group)
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
			"group": group,
		}).Error("error getting status for consumer group. returning.")
		return
	}

	for _, partition := range status.Status.Partitions {
	  metrics := make([]graphite.Metric, 1)
	  metricNamePrefix := "services.burrow" + "." + status.Status.Cluster + "." + "group" + "." + status.Status.Group + "." + "topic" + "." + partition.Topic + "." + strconv.Itoa(int(partition.Partition))
	  metrics[0] = graphite.NewMetric(metricNamePrefix + "." + "Lag", strconv.Itoa(int(partition.End.Lag)), time.Now().Unix())

    // fmt.Printf("%v\n", metrics)
	  err := gh.SendMetrics(metrics)
	  if err != nil {
    		log.WithFields(log.Fields{
    			"err": err,
    		}).Error("Error in sending metrics to Graphite. returning.")
    		return
    }
/*
    fmt.Printf("Group: %s, partition %d, endLag: %d, group, int(partition.Partition),
      int(partition.End.Lag)
*/
	}

  totalLagMetricName := "services.burrow" + "." + status.Status.Cluster + "." + "group" + "." + status.Status.Group + "." + "TotalLag"
  err = gh.SimpleSend(totalLagMetricName, strconv.Itoa(int(status.Status.TotalLag)))
  if err != nil {
      log.WithFields(log.Fields{
        "err": err,
      }).Error("Error in sending metrics to Graphite. returning.")
      return
  }
  // fmt.Printf("MetricName: %s, value: %s\n", totalLagMetricName, strconv.Itoa(int(status.Status.TotalLag)))
}

func (be *BurrowExporter) processCluster(cluster string, gh *graphite.Graphite) {
	groups, err := be.client.ListConsumers(cluster)
	if err != nil {
		log.WithFields(log.Fields{
			"err":     err,
			"cluster": cluster,
		}).Error("error listing consumer groups. returning.")
		return
	}

	wg := sync.WaitGroup{}

	for _, group := range groups.ConsumerGroups {
		wg.Add(1)

		go func(g string) {
			defer wg.Done()
			be.processGroup(cluster, g, gh)
		}(group)
	}

	wg.Wait()
}

func (be *BurrowExporter) startGraphite() {
	// http.Handle("/graphite-metrics", promhttp.Handler())
	// go http.ListenAndServe(be.metricsListenAddr, nil)
}

func (be *BurrowExporter) Close() {
	be.wg.Wait()
}

func (be *BurrowExporter) Start(ctx context.Context) {
	be.startGraphite()

	be.wg.Add(1)
	defer be.wg.Done()

	be.mainLoop(ctx)
}

func (be *BurrowExporter) scrape() {
	start := time.Now()
	log.WithField("timestamp", start.UnixNano()).Info("Scraping burrow...")

	log.Info("Getting connection to the Graphite server")
	gh, err := GetGraphiteConnection(be.graphiteHost, be.graphitePort)
  if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("error Connnecting to Graphite server. Continuing.")
		return
	}

	clusters, err := be.client.ListClusters()
	if err != nil {
		log.WithFields(log.Fields{
			"err": err,
		}).Error("error listing clusters. Continuing.")
		return
	}

	wg := sync.WaitGroup{}

	for _, cluster := range clusters.Clusters {
		wg.Add(1)

		go func(c string) {
			defer wg.Done()
			be.processCluster(c, gh)
		}(cluster)
	}

	wg.Wait()

  log.Info("Closing the connection to the Graphite server")
	err = CloseGraphiteConnection(gh)
	if err != nil {
  		log.WithFields(log.Fields{
  			"err": err,
  		}).Error("error Closing the connection to Graphite server. Continuing.")
  		return
  }

	end := time.Now()
	log.WithFields(log.Fields{
		"timestamp": end.UnixNano(),
		"took":      end.Sub(start),
	}).Info("Finished scraping burrow.")
}

func (be *BurrowExporter) mainLoop(ctx context.Context) {
	timer := time.NewTicker(time.Duration(be.interval) * time.Second)

	// scrape at app start without waiting for the first interval to elapse
	be.scrape()

	for {
		select {
		case <-ctx.Done():
			log.Info("Shutting down exporter.")
			timer.Stop()
			return

		case <-timer.C:
			be.scrape()
		}
	}
}

func MakeBurrowExporter(burrowUrl string, graphiteHost string, graphitePort int, interval int) *BurrowExporter {
	return &BurrowExporter{
		client:            MakeBurrowClient(burrowUrl),
		graphiteHost:      graphiteHost,
		graphitePort:      graphitePort,
		interval:          interval,
	}
}
