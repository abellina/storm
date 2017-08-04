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
class TimeSeries {

    constructor(element) {
        this.element = element;
        this.intervalId = null;
        this.update = null;
    }

    setWindow(window) {
        this.window = window;
    }

    setInterval(newInterval) {
        this.interval = newInterval || 5000;
        if (this.intervalId !== null) {
            clearInterval(this.intervalId);
            this.startInterval();
        }
    }

    setRange(range) {
        this.range = range;
    }

    setLegend(names) {
        this.names = names;
    }

    setUpdateFunction(func) {
        this.update = func;
    }

    startInterval() {
        var element = this.element;
        var updateFunction = this.update;
        var range = this.range;
        var updatePlot = this.updatePlot;

        this.intervalId = setInterval(function () {
            updatePlot(updateFunction, element, range);
        }, this.interval);
    }

    updatePlot(updateFunction, element, range) {
        var time = new Date();

        var x_vals = [];
        var modes = [];
        var indices = [];
        var y_vals = updateFunction();

        for (var i = 0; i < y_vals.length; ++i) {
            x_vals.push([time]);
            modes.push(["lines"]);
            indices.push(i);
        }

        var data = {
            x: x_vals,
            y: y_vals,
            mode: modes
        };

        var rangeMins = range / 60;
        var olderTime = time.setMinutes(time.getMinutes() - rangeMins);
        var futureTime = time.setMinutes(time.getMinutes() + rangeMins);
        var view = {
            xaxis: {
                type: 'date',
                range: [olderTime, futureTime]
            }
        };
        Plotly.relayout(element, view);
        Plotly.extendTraces(element, data, indices);
    }

    plot() {

        var time = new Date();
        // initial data requires a slightly different format
        var yvals = this.update();
        var data = [];
        for (var i in yvals) {
            var yval = yvals[i];
            data.push({
                x: [time],
                y: [yval],
                mode: ['lines'],
                name: this.names[i]
            });
        }

        Plotly.plot(this.element, data);

        this.updatePlot(this.update, this.element, this.range);
        this.startInterval();
    }


}