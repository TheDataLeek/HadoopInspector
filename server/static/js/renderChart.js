function addGraphTo(location, datafunc) {
    var chart = nv.models.lineChart()
                .margin({left: 100, right:50})
                //.useInteractiveGuideline(true)
                .duration(350)
                .showLegend(true)
                .showXAxis(true)
                .showYAxis(true);

    var format = d3.time.format("%Y-%m-%d");
    chart.xAxis
            .axisLabel('Date')
            .tickFormat(function(d) { return format(new Date(d)); });

    chart.yAxis
            .axisLabel('Violations')
            .tickFormat(d3.format('02d'));

    var data = datafunc();

    d3.select(location)
            .datum(data)
            .call(chart);

    nv.utils.windowResize( function() { chart.update() });

    return chart;
};
