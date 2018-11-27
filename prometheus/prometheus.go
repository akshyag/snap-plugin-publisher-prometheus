package prometheus

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	log "github.com/sirupsen/logrus"

	"github.com/intelsdi-x/snap-plugin-lib-go/v1/plugin"
	"github.com/intelsdi-x/snap/core"
)

const (
	Name    = "prometheus"
	Version = 1
)

var (
	invalidMetric = regexp.MustCompile("[^a-zA-Z0-9:_]")
	invalidLabel  = regexp.MustCompile("[^a-zA-Z0-9_]")
)

//NewPrometheusPublisher returns an instance of the Prometheus publisher
func NewPrometheusPublisher() *PrometheusPublisher {
	return &PrometheusPublisher{}
}

type PrometheusPublisher struct {
}

type configuration struct {
	host, logLevel string
	port           int64
	https, debug   bool
}

func getConfig(config plugin.Config) (configuration, error) {
	cfg := configuration{}
	var err error

	cfg.host, err = config.GetString("host")
	if err != nil {
		return cfg, fmt.Errorf("%s: %s", err, "host")
	}

	cfg.logLevel, err = config.GetString("log-level")
	if err != nil {
		cfg.logLevel = "undefined"
	}

	cfg.port, err = config.GetInt("port")
	if err != nil {
		return cfg, fmt.Errorf("%s: %s", err, "port")
	}

	cfg.https, err = config.GetBool("https")
	if err != nil {
		return cfg, fmt.Errorf("%s: %s", err, "https")
	}

	cfg.debug, err = config.GetBool("debug")
	if err != nil {
		return cfg, fmt.Errorf("%s: %s", err, "debug")
	}

	return cfg, nil
}

func (p *PrometheusPublisher) GetConfigPolicy() (plugin.ConfigPolicy, error) {
	policy := plugin.NewConfigPolicy()
	fmt.Println("Came to get config Policy")

	policy.AddNewStringRule([]string{""}, "host", true)
	policy.AddNewIntRule([]string{""}, "port", true)
	policy.AddNewBoolRule([]string{""}, "https", false, plugin.SetDefaultBool(false))
	policy.AddNewBoolRule([]string{""}, "debug", false, plugin.SetDefaultBool(false))

	fmt.Println("done with sending config Policy")
	return *policy, nil
}

func (p *PrometheusPublisher) Publish(metrics []plugin.Metric, pluginConfig plugin.Config) error {
	config, err := getConfig(pluginConfig)
	if err != nil {
		return err
	}

	logger := getLogger(config)

	promUrl, err := prometheusUrl(config)
	if err != nil {
		panic(err)
	}

	logger.Debug("Done with getting the URL")
	sendMetrics(config, promUrl, metrics)
	logger.Debug("Done with posting the metrics")
	return nil
}

func sendMetrics(config configuration, promUrl string, metrics []plugin.Metric) {
	logger := getLogger(config)
	for _, m := range metrics {
		//Get name tags and value. ts is not used anymore in new prometheus pushgateway
		name, tags, value, err := mangleMetric(m)

		if err == nil {
			metric := prometheus.NewGauge(prometheus.GaugeOpts{
				Name: name,
				Help: fmt.Sprintf("Collected metric %v", name),
			})

			metric.Set(value)

			pusher := GetTaggedPusher(push.New(promUrl, name).Collector(metric), tags)
			if err := pusher.Push(); err != nil {
				logger.Error("Error pushing to Prometheus pushgatewa: %v %v %v", name, tags, value)
			}
		} else {
			logger.Error("Error in decoding the metric ", err)
		}
	}
}

func prometheusString(name string, tags map[string]string, value string, ts int64) string {
	tmp1 := []string{}
	for k, v := range tags {
		tmp1 = append(tmp1, fmt.Sprintf("%s=\"%s\"", k, v))
	}
	return fmt.Sprintf("%s{%s} %s %d",
		name,
		strings.Join(tmp1, ","),
		value,
		ts,
	)
}

func mangleMetric(m plugin.Metric) (name string, tags map[string]string, value float64, err error) {
	tags = make(map[string]string)
	ns := m.Namespace.Strings()
	isDynamic, indexes := m.Namespace.IsDynamic()
	if isDynamic {
		for i, j := range indexes {
			// The second return value from IsDynamic(), in this case `indexes`, is the index of
			// the dynamic element in the unmodified namespace. However, here we're deleting
			// elements, which is problematic when the number of dynamic elements in a namespace is
			// greater than 1. Therefore, we subtract i (the loop iteration) from j
			// (the original index) to compensate.
			//
			// Remove "data" from the namespace and create a tag for it
			ns = append(ns[:j-i], ns[j-i+1:]...)
			tags[m.Namespace[j].Name] = m.Namespace[j].Value
		}
	}

	for i, v := range ns {
		ns[i] = invalidMetric.ReplaceAllString(v, "_")
	}

	// Add "unit"" if we do not already have a "unit" tag
	if _, ok := m.Tags["unit"]; !ok {
		tags["unit"] = m.Unit
	}

	// Process the tags for this metric
	for k, v := range m.Tags {
		// Convert the standard tag describing where the plugin is running to "source"
		if k == core.STD_TAG_PLUGIN_RUNNING_ON {
			// Unless the "source" tag is already being used
			if _, ok := m.Tags["source"]; !ok {
				tags["source"] = v
			}
			if _, ok := m.Tags["host"]; !ok {
				tags["host"] = v
			}
		} else {
			tags[invalidLabel.ReplaceAllString(k, "_")] = v
		}
	}

	name = strings.Join(ns, "_")

	//Convert the sent data to float64 if possible
	//The data is streamed as a json string. so need to correctly format the string and convert to float

	data := fmt.Sprintf("%v", m.Data)
	dataFloat, e := strconv.ParseFloat(data, 64)

	if e != nil {
		err = errors.New(fmt.Sprintf("Unsupported metric data for prometheus %v:%#v", name, m.Data))
	} else {
		value = dataFloat
	}
	return
}

func prometheusUrl(config configuration) (string, error) {
	var prefix = "http"
	if config.https {
		prefix = "https"
	}

	u, err := url.Parse(fmt.Sprintf("%s://%s:%d", prefix, config.host, config.port))
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func GetTaggedPusher(p *push.Pusher, tags map[string]string) *push.Pusher {
	//Go through the tags and generate a pusher using Grouping method
	tmpPusher := p
	for k, v := range tags {
		tmpPusher = tmpPusher.Grouping(k, v)
	}
	return tmpPusher
}

func getLogger(config configuration) *log.Entry {
	logger := log.WithFields(log.Fields{
		"plugin-name":    Name,
		"plugin-version": Version,
		"plugin-type":    "publisher",
	})

	// default
	log.SetLevel(log.WarnLevel)

	if config.debug {
		log.SetLevel(log.DebugLevel)
		return logger
	}
	return logger
}
