// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	// The value of "type" key in configuration.
	Type = "newrelic"
	// The stability level of the exporter.
	stability = component.StabilityLevelBeta
	
	defaultTimeout = 30 * time.Second
)

// NewFactory creates a factory for New Relic exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(Type),
		createDefaultConfig,
		exporter.WithMetrics(createMetricsExporter, stability),
		exporter.WithLogs(createLogsExporter, stability),
		exporter.WithTraces(createTracesExporter, stability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		ClientConfig: confighttp.ClientConfig{
			Timeout: defaultTimeout,
		},
		TimeoutSettings: exporterhelper.TimeoutConfig{Timeout: defaultTimeout},
		QueueSettings:   exporterhelper.NewDefaultQueueConfig(),
		CommonAttributes: make(map[string]interface{}),
	}
}

func createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	nrCfg := cfg.(*Config)
	return newMetricsExporter(nrCfg, set)
}

func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	nrCfg := cfg.(*Config)
	return newLogsExporter(nrCfg, set)
}

func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	nrCfg := cfg.(*Config)
	return newTracesExporter(nrCfg, set)
}
