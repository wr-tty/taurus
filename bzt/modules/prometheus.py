# coding=utf-8

from bzt.engine import Reporter, Singletone
from bzt.modules.aggregator import AggregatorListener, ResultsProvider
from bzt.six import iteritems
from prometheus_client import Gauge, Histogram, start_http_server, Counter, Summary


class PrometheusStatusReporter(Reporter, AggregatorListener, Singletone):

    def __init__(self):
        super(PrometheusStatusReporter, self).__init__()
        self.kpi_buffer = []
        self.port = '9090'
        self.bztTestSendBytes = None
        self.bztTestAvgResponseTimeSeconds = None
        self.bztTestAvgLatencyTimeSeconds = None
        self.bztTestAvgConnectionTimeSeconds = None
        self.bztTestConcurrentUsers = None
        self.bztTestSuccessRequestCount = None
        self.bztTestErrorRequestCount = None
        self.bztResponseTimeSeconds = None
        self.bztConnectionTimeSeconds = None
        self.bztLatencyTimeSeconds = None
        self.bztTestSendBytesGauge = None
        self.bztTestThroughput = None

    def prepare(self):
        """
        Read options for reporting, check that they're sane
        """
        super(PrometheusStatusReporter, self).prepare()
        if isinstance(self.engine.aggregator, ResultsProvider):
            self.engine.aggregator.add_listener(self)

        self.port = self.settings.get("port", self.port)
        if 1:
            self.log.warning("Set up Prometheus variables. Continue ...")
            self.log.info("port: %s", self.port)

    def startup(self):
        """
        Initiate report metrics
        """
        super(PrometheusStatusReporter, self).startup()
        # setup common labels for metrics
        common_labels = (
            'test_label',
            # 'throughput',
            # 'concurrency',
            # 'succ',
            # 'fail',
            'response_code',
            # 'avg_rt',
            # 'stdev_rt',
            # 'avg_lt',
            # 'avg_ct',
            # 'bytes',
            # 'errors',
            # 'rt',
            # 'rc',
            # 'perc'
        )
        print(self.engine.__module__)
        # Start up the server to expose the metrics.
        start_http_server(self.port)
        # setup metrics instance
        self.bztTestSendBytes = Histogram(
            'bzt_test_send_bytes',
            'Provide BlazeMeter test send bytes results',
            common_labels,
        )
        self.bztTestAvgResponseTimeSeconds = Histogram(
            'bzt_test_avg_response_time_seconds',
            'Provide BlazeMeter test average response time results',
            common_labels,
        )
        self.bztTestAvgLatencyTimeSeconds = Histogram(
            'bzt_test_avg_latency_time_seconds',
            'Provide BlazeMeter test average latency time results',
            common_labels,
        )
        self.bztTestAvgConnectionTimeSeconds = Histogram(
            'bzt_test_avg_connection_time_seconds',
            'Provide BlazeMeter test average connection time results',
            common_labels,
        )
        self.bztTestConcurrentUsers = Gauge(
            'bzt_test_concurrent_users',
            'Provide BlazeMeter test concurrent users',
            common_labels,
        )
        self.bztTestSuccessRequestCount = Counter(
            'bzt_test_success_request',
            'Provide BlazeMeter test success request count',
            common_labels,
        )
        self.bztTestErrorRequestCount = Counter(
            'bzt_test_error_request',
            'Provide BlazeMeter test error request count',
            common_labels,
        )
        self.bztResponseTimeSeconds = Gauge(
            'bzt_test_response_time_seconds',
            'Provide BlazeMeter test response times',
            common_labels,
        )
        self.bztConnectionTimeSeconds = Gauge(
            'bzt_test_connection_time_seconds',
            'Provide BlazeMeter test connection times',
            common_labels,
        )
        self.bztLatencyTimeSeconds = Gauge(
            'bzt_test_latency_time_seconds',
            'Provide BlazeMeter test latency times',
            common_labels,
        )
        self.bztTestSendBytesGauge = Gauge(
            'bzt_test_send_bytes_gauge',
            'Provide BlazeMeter test send bytes',
            common_labels,
        )
        self.bztTestThroughput = Counter(
            'bzt_test_throughput',
            'Provide BlazeMeter throughput',
            common_labels,
        )
        self.log.info("Started prometheus http server at port: %s", self.port)

    def check(self):
        """
        Send data
        """
        self.log.debug("KPI bulk buffer len: %s", len(self.kpi_buffer))
        if len(self.kpi_buffer):
            self.__send_data(self.kpi_buffer)
            self.kpi_buffer = []

            # self.__send_monitoring()
        return super(PrometheusStatusReporter, self).check()

    def __send_data(self, data):
        """
        :type data: list[bzt.modules.aggregator.DataPoint]
        """
        for dataRow in data:
            for test_label, values in iteritems(dataRow['current']):
                if len(test_label) > 0:
                    self.assign_values(test_label, values)

    def assign_values(self, test_label, values):
        return_code = values['rc'].elements()
        label_values = {
            'test_label': test_label,
            'response_code': next(return_code)
        }
        bzt_test_send_bytes_instance = self.bztTestSendBytes.labels(**label_values)
        bzt_test_send_bytes_instance.observe(values['bytes'])
        bzt_test_avg_connection_time = self.bztTestAvgConnectionTimeSeconds.labels(**label_values)
        bzt_test_avg_connection_time.observe(values['avg_ct'])
        bzt_test_avg_latency_time = self.bztTestAvgLatencyTimeSeconds.labels(**label_values)
        bzt_test_avg_latency_time.observe(values['avg_lt'])
        bzt_test_avg_response_time = self.bztTestAvgResponseTimeSeconds.labels(**label_values)
        bzt_test_avg_response_time.observe(values['avg_rt'])
        bzt_test_concurrent_users = self.bztTestConcurrentUsers.labels(**label_values)
        bzt_test_concurrent_users.set(values['concurrency'])
        bzt_test_success_request_count = self.bztTestSuccessRequestCount.labels(**label_values)
        bzt_test_success_request_count.inc(values['succ'])
        bzt_test_error_request_count = self.bztTestErrorRequestCount.labels(**label_values)
        bzt_test_error_request_count.inc(values['fail'])
        bzt_test_response_time = self.bztResponseTimeSeconds.labels(**label_values)
        bzt_test_response_time.set(values['avg_rt'])
        bzt_test_latency_time = self.bztLatencyTimeSeconds.labels(**label_values)
        bzt_test_latency_time.set(values['avg_lt'])
        bzt_test_connection_time = self.bztConnectionTimeSeconds.labels(**label_values)
        bzt_test_connection_time.set(values['avg_ct'])
        bzt_test_send_bytes_gauge = self.bztTestSendBytesGauge.labels(**label_values)
        bzt_test_send_bytes_gauge.set(values['bytes'])
        bzt_test_throughput = self.bztTestThroughput.labels(**label_values)
        bzt_test_throughput.inc(values['throughput'])

    def post_process(self):
        self.log.info("Sending remaining KPI data to prometheus...")
        self.__send_data(self.kpi_buffer)
        self.kpi_buffer = []

    def aggregated_second(self, data):
        self.kpi_buffer.append(data)

    def aggregated_results(self, results, cumulative_results):
        pass
