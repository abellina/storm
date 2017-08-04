/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class MetricGraph {

    getMetricNames(data) {
        var ret = [];
        var parts = data.split('&');
        for (var i in parts) {
            var metric_str = parts[i];
            if (metric_str.startsWith('metric_name=')) {
                var metric_name = metric_str.split('=')[1];
                ret.push(metric_name);
            }
        }
        return ret;
    }

    constructor(data) {

        this.graph = null;
        this.url = null;

        this.topologyId = this.getParameter('topology', data);
        this.metrics = this.getMetricNames(data);
        this.graph_type = this.getParameter('graph_type', data);
        this.aggregation_type = this.getParameter('aggregation_type', data);

        var now = new Date().getTime();
        var startTime = this.getParameter('starttime', data);
        var endTime = this.getParameter('endtime', data);

        this.start = (startTime) ? new Date(startTime).getTime() : now - (3 * 60 * 60 * 1000); // 3 hrs
        this.end = (endTime) ? new Date(endTime).getTime() : now;

        switch (this.graph_type) {
            case 'line':
                this.points = parseInt(this.getParameter('points', data));
                break;
            case 'bar':
                this.stacked = this.getParameter('stacked_grouped', data) === 'stacked';
                this.buckets = parseInt(this.getParameter('buckets', data));
                break;
            case 'stream':
                this.frequency = parseInt(this.getParameter('frequency', data));
                this.range = parseInt(this.getParameter('range', data));
                this.window = parseInt(this.getParameter('window', data));
                break;
        }
    }


    setElement(element) {
        this.element = element;
    }

    setDataUrl(url) {
        this.url = url;
    }

    // https://stackoverflow.com/questions/901115/
    // how-can-i-get-query-string-values-in-javascript?page=1&tab=votes#tab-top
    getParameter(name, datastr) {
        name = name.replace(/[\[\]]/g, "\\$&");
        var regex = new RegExp("[?&]" + name + "(=([^&#]*)|&|#|$)"),
            results = regex.exec(datastr);
        if (!results) return null;
        if (!results[2]) return '';
        return decodeURIComponent(results[2].replace(/\+/g, " "));
    }

    formatDate(timestamp) {
        var m = moment(timestamp);
        return m.format("MM/DD H:mm a");
    }

    executeQuery(params, url) {
        var results = null;

        $.ajax({
            type: 'POST',
            url: url,
            data: params,
            success: function (data, status, jqXHR) {
                results = data;
            },
            async: false
        });

        return results;
    }

    getGraph() {

        if (this.url === null)
            return;

        var params = {
            topology_id: this.topologyId,
            op: this.aggregation_type,
            metrics: this.metrics,
            start_times: [this.start],
            end_times: [this.end]
        };

        switch (this.graph_type) {
            case 'bar':

                // divide time range into buckets
                var start_times = [], end_times = [];
                var bucket_size = (this.end - this.start) / this.buckets;
                for (var i = this.start; i < this.end; i += bucket_size) {
                    start_times.push(i);
                    end_times.push(i + bucket_size);
                }
                params["start_times"] = start_times;
                params["end_times"] = end_times;

                var results = this.executeQuery(params, this.url);
                var g = new Bar(this.element);
                g.setStacked(this.stacked);


                // format timestamps into pretty dates
                var start_times_formatted = [];
                for (var i in start_times) {
                    var formatted = this.formatDate(start_times[i]);
                    start_times_formatted.push(formatted);
                }

                for (var i in this.metrics) {
                    var metric = this.metrics[i];

                    var bar_values = new Array(results.length).fill(0);

                    for (var r in results) {
                        var values = results[r].values;
                        if (metric in values)
                            bar_values[r] = values[metric];
                    }

                    g.addCategory({
                        x: start_times_formatted,
                        y: bar_values,
                        name: metric
                    });

                }

                return g;

            case 'line':
                // divide time range into buckets
                var start_times = [], end_times = [];
                var point_size = (this.end - this.start) / this.points;
                for (var i = this.start; i < this.end; i += point_size) {
                    start_times.push(i);
                    end_times.push(i + point_size);
                }
                params["start_times"] = start_times;
                params["end_times"] = end_times;

                var results = this.executeQuery(params, this.url);
                var g = new Line(this.element);

                // format timestamps into pretty dates
                var start_times_formatted = [];
                for (var i in start_times) {
                    var formatted = this.formatDate(start_times[i]);
                    start_times_formatted.push(formatted);
                }

                for (var i in this.metrics) {
                    var metric = this.metrics[i];

                    var line_values = new Array(results.length).fill(0);

                    for (var r in results) {
                        var values = results[r].values;
                        if (metric in values)
                            line_values[r] = values[metric];
                    }

                    g.addCategory({
                        x: start_times_formatted,
                        y: line_values,
                        name: metric
                    });

                }

                return g;

            case 'stream':
                var executeQuery = this.executeQuery;
                var metrics = this.metrics;
                var url = this.url;
                var freq = this.frequency;
                var window = this.window;
                var range = this.range;
                var now = new Date().getTime();
                params["start_times"] = [now - freq];

                params["end_times"] = [now];
                var g = new TimeSeries(this.element);
                g.setLegend(metrics);
                g.setWindow(window);
                g.setRange(range);
                g.setInterval(freq);

                g.setUpdateFunction(function () {
                    var ret = [];
                    now = new Date().getTime();
                    params["start_times"] = [now - (window * 1000)];
                    params["end_times"] = [now];
                    results = executeQuery(params, url);
                    var values = results[0].values;

                    for (var i in metrics) {
                        var metric = metrics[i];
                        var value = (metric in values && values[metric]) ? values[metric] : 0;
                        ret.push([value]);
                    }
                    return ret;
                });

                return g;

            case 'pie':
                var results = this.executeQuery(params, this.url);
                var g = new Pie(this.element);
                for (var i in this.metrics) {
                    var metric = this.metrics[i];
                    var values = results[0].values;
                    if (metric in values && values[metric] !== 0) {
                        g.addCategory(metric, values[metric]);
                    }
                }
                return g;

            case 'default':
                alert("No graph type specified!");
                return null;
        }
    }

}
