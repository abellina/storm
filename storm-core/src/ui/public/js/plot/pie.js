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
class Pie {

    constructor(element) {
        this.element = element;
        this.values = [];
        this.labels = [];

        this.layout = {
            annotations: [
                {
                    font: {
                        size: 20
                    },
                    showarrow: false,
                    x: 0.17,
                    y: 0.5,
                    text: ''
                }
            ]
        };
    }

    addCategory(label, value){
        this.labels.push(label);
        this.values.push(value);
    }

    plot() {

        this.data = [{
            values: this.values,
            labels: this.labels,
            domain: {
                x: [0, .48]
            },
            hoverinfo: 'label+value+percent',
            hole: .4,
            type: 'pie'
        }];

        Plotly.newPlot(this.element, this.data, this.layout);
    }

}
