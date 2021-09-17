/*
 * Copyright 2021 Guo Chaosheng
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.nopasserby.fastminiraft.benchmark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ThroughputChart {
    
    static String ls = System.lineSeparator();
    
    static String[] keywords = { "TPS" };
    
    static String template;
    
    static {
        template = "<html>                                                                                                    " + ls
                 + "<head><meta charset='UTF-8'></head>                                                                       " + ls
                 + "<body>                                                                                                    " + ls
                 + "<div id='container' style='height: 100%'></div>                                                           " + ls
                 + "                                                                                                          " + ls
                 + "<script type='text/javascript' src='https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js'></script> " + ls
                 + "<script type='text/javascript'>                                                                           " + ls
                 + "var dom = document.getElementById('container');                                                           " + ls
                 + "var chart = echarts.init(dom);                                                                            " + ls
                 + "chart.setOption({                                                                                         " + ls
                 + "    xAxis: {                                                                                              " + ls
                 + "        type: 'category',                                                                                 " + ls
                 + "        data: ${datetimeArray}                                                                            " + ls
                 + "    },                                                                                                    " + ls
                 + "    yAxis: {                                                                                              " + ls
                 + "        type: 'value'                                                                                     " + ls
                 + "    },                                                                                                    " + ls
                 + "    series: [{                                                                                            " + ls
                 + "        data: ${valueArray},                                                                              " + ls
                 + "        type: 'line'                                                                                      " + ls
                 + "    }]                                                                                                    " + ls
                 + "});                                                                                                       " + ls
                 + "</script>                                                                                                 " + ls
                 + "</body>                                                                                                   " + ls
                 + "</html>                                                                                                   " + ls;
    }
    
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("at least one log file path");
            return;
        }
        
        for (String keyword: keywords) {
            stdoutHtmlChart(args, keyword);
        }
    }
    
    public static void stdoutHtmlChart(String[] sources, String keyword) throws IOException {  
        List<String> datetimeArray = new ArrayList<String>();
        List<String> valueArray = new ArrayList<String>();
        
        for (String source: sources) {
            String line = null;
            try (BufferedReader br = new BufferedReader(new FileReader(source))) {
                while ((line = br.readLine()) != null) {
                    if (!line.contains(keyword)) {
                        continue;
                    }
                    
                    String datetimeText = "\"" + line.substring(0, 8) + "\"";
                    datetimeArray.add(datetimeText);
                    
                    int fromIndex = line.indexOf(keyword) + keyword.length();
                    int startOf = line.indexOf(" ", fromIndex) + 1;
                    int endOf = line.indexOf(" ", startOf);
                    String valueText = line.substring(startOf, endOf);
                    valueArray.add(valueText);
                }
            }
        }
        
        String result = template.replaceAll("\\$\\{datetimeArray\\}", datetimeArray.toString())
                                .replaceAll("\\$\\{valueArray\\}", valueArray.toString());
        
        System.out.println("============================================================");
        System.out.println(result);
    }
    
}
