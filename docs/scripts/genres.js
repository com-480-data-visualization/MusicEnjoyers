(function() {
d3.json("data/genre_stats.json").then(function(jsonData) {
    const genreData = jsonData.data;
    const keys = jsonData.keys;

    const colorScale = d3.scaleOrdinal()
        .domain(keys)
        .range(["#1DB954", "#1ED760", "#3D9E4F", "#61D56F", "#55B37A", "#4D8A5E", "#2C7D4F", "#7DDC92", "#8FD7A9", "#76C384"]);

    const margin = { top: 30, right: 20, bottom: 40, left: 60 };
    const width = 760 - margin.left - margin.right;
    const height = 500 - margin.top - margin.bottom;
    const legendWidth = 180;

    const svg = d3.select("#genreChart")
        .append("svg")
        .attr("width", width + margin.left + margin.right + legendWidth)
        .attr("height", height + margin.top + margin.bottom)
        .style("padding", "20px")
        .style("background", "#181818")
        .style("border-radius", "16px")
        .style("box-shadow", "0 12px 30px rgba(0,0,0,0.25)")
        .append("g")
        .attr("transform", `translate(${margin.left},${margin.top})`);

    const stack = d3.stack()
        .keys(keys)
        .order(d3.stackOrderNone)
        .offset(d3.stackOffsetExpand);

    const layers = stack(genreData);

    const xScale = d3.scaleLinear()
        .domain(d3.extent(genreData, d => d.year))
        .range([0, width]);

    const yScale = d3.scaleLinear()
        .domain([0, 1])
        .range([height, 0]);

    const area = d3.area()
        .x(d => xScale(d.data.year))
        .y0(d => yScale(d[0]))
        .y1(d => yScale(d[1]))
        .curve(d3.curveCardinal);

    svg.selectAll(".layer")
        .data(layers)
        .enter()
        .append("path")
        .attr("class", "layer")
        .attr("d", area)
        .attr("fill", d => colorScale(d.key))
        .attr("opacity", 0.85);

    const xAxis = d3.axisBottom(xScale).ticks(10).tickFormat(d3.format("d"));
    const yAxis = d3.axisLeft(yScale).ticks(5).tickFormat(d3.format(".0%"));

    svg.append("g")
        .attr("transform", `translate(0, ${height})`)
        .call(xAxis)
        .selectAll("text")
        .style("fill", "#B3B3B3");

    svg.append("g")
        .call(yAxis)
        .selectAll("text")
        .style("fill", "#B3B3B3");

    svg.selectAll(".domain, .tick line")
        .style("stroke", "#444");

    const legend = svg.append("g")
        .attr("transform", `translate(${width + 20}, 30)`);

    keys.forEach((key, index) => {
        const legendRow = legend.append("g")
            .attr("transform", `translate(0, ${index * 24})`);

        legendRow.append("rect")
            .attr("width", 12)
            .attr("height", 12)
            .attr("fill", colorScale(key));

        legendRow.append("text")
            .attr("x", 18)
            .attr("y", 10)
            .style("fill", "#B3B3B3")
            .style("font-size", "11px")
            .text(key);
    });
});
})();