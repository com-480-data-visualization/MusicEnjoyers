(function() {
const genreData = [
    { year: 2016, country: 15, christmas: 4, "adult standards": 7, rap: 8, rockabilly: 5, "r&b": 6, "doo-wop": 8, "acoustic country": 6, "hip hop": 4, "melodic rap": 2 },
    { year: 2017, country: 21, christmas: 8, "adult standards": 11, rap: 11, rockabilly: 4, "r&b": 7, "doo-wop": 5, "acoustic country": 7, "hip hop": 6, "melodic rap": 5 },
    { year: 2018, country: 21, christmas: 8, "adult standards": 7, rap: 14, rockabilly: 4, "r&b": 5, "doo-wop": 5, "acoustic country": 7, "hip hop": 8, "melodic rap": 3 },
    { year: 2019, country: 23, christmas: 12, "adult standards": 11, rap: 10, rockabilly: 6, "r&b": 7, "doo-wop": 6, "acoustic country": 9, "hip hop": 3, "melodic rap": 5 },
    { year: 2020, country: 20, christmas: 11, "adult standards": 12, rap: 9, rockabilly: 7, "r&b": 8, "doo-wop": 6, "acoustic country": 5, "hip hop": 3, "melodic rap": 9 },
    { year: 2021, country: 24, christmas: 11, "adult standards": 9, rap: 10, rockabilly: 7, "r&b": 10, "doo-wop": 8, "acoustic country": 7, "hip hop": 3, "melodic rap": 7 },
    { year: 2022, country: 24, christmas: 14, "adult standards": 14, rap: 12, rockabilly: 8, "r&b": 9, "doo-wop": 7, "acoustic country": 5, "hip hop": 5, "melodic rap": 4 },
    { year: 2023, country: 22, christmas: 12, "adult standards": 13, rap: 7, rockabilly: 8, "r&b": 4, "doo-wop": 6, "acoustic country": 3, "hip hop": 2, "melodic rap": 2 },
    { year: 2024, country: 17, christmas: 13, "adult standards": 9, rap: 10, rockabilly: 10, "r&b": 6, "doo-wop": 9, "acoustic country": 1, "hip hop": 4, "melodic rap": 1 },
    { year: 2025, country: 23, christmas: 12, "adult standards": 9, rap: 6, rockabilly: 9, "r&b": 5, "doo-wop": 7, "acoustic country": 3, "hip hop": 3, "melodic rap": 1 }
];

const keys = [
    "country",
    "christmas",
    "adult standards",
    "rap",
    "rockabilly",
    "r&b",
    "doo-wop",
    "acoustic country",
    "hip hop",
    "melodic rap"
];

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

svg.append("text")
    .attr("x", width / 2)
    .attr("y", -10)
    .attr("text-anchor", "middle")
    .style("fill", "#FFFFFF")
    .style("font-size", "18px")
    .style("font-weight", "700")
    .text("Genre Share by Year (100% stacked)");

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
})();