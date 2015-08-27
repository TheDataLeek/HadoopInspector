function addGraphTo(location) {
    var chart = nv.models.lineChart()
                .margin({left: 100, right:50})
                //.useInteractiveGuideline(true)
                .duration(350)
                .showLegend(true)
                .showXAxis(true)
                .showYAxis(true);

    chart.xAxis
            .axisLabel('Date')
            .tickFormat(function(d) { return d3.time.format("%Y-%m-%d")(new Date(d)); });

    chart.yAxis
            .axisLabel('Violations')
            .tickFormat(d3.format('02d'));

    var data = convertData();

    d3.select(location)
            .datum(data)
            .call(chart);

    // Resize graph on window resize
    nv.utils.windowResize( function() { chart.update() });

    return chart;
};
