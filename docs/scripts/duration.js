(function() {
// Dummy data for song duration from 2015 to 2025
const durationData = [
    { year: 2015, duration: 10 },
    { year: 2016, duration: 9 },
    { year: 2017, duration: 8 },
    { year: 2018, duration: 7 },
    { year: 2019, duration: 6 },
    { year: 2020, duration: 5 },
    { year: 2021, duration: 4 },
    { year: 2022, duration: 3 },
    { year: 2023, duration: 2 },
    { year: 2024, duration: 1 },
    { year: 2025, duration: 1 }
];

// Set dimensions
const margin = { top: 20, right: 20, bottom: 40, left: 50 };
const width = 600 - margin.left - margin.right;
const height = 400 - margin.top - margin.bottom;

// Create SVG
const svg = d3.select("#duration-bar")
    .append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", `translate(${margin.left},${margin.top})`);

// Create scales
const xScale = d3.scaleBand()
    .domain(durationData.map(d => d.year))
    .range([0, width])
    .padding(0.1);

const yScale = d3.scaleLinear()
    .domain([0, d3.max(durationData, d => d.duration)])
    .range([height, 0]);

// Create bars with gradient effect
svg.selectAll(".bar")
    .data(durationData)
    .enter()
    .append("rect")
    .attr("class", "bar")
    .attr("x", d => xScale(d.year))
    .attr("y", d => yScale(d.duration))
    .attr("width", xScale.bandwidth())
    .attr("height", d => height - yScale(d.duration))
    .attr("fill", "#1DB954")
    .attr("opacity", 0.8)
    .transition()
    .duration(1000)
    .attr("opacity", 1);

// Add X axis
svg.append("g")
    .attr("transform", `translate(0,${height})`)
    .call(d3.axisBottom(xScale))
    .style("color", "#B3B3B3")
    .style("font-size", "12px");

// Add Y axis
svg.append("g")
    .call(d3.axisLeft(yScale))
    .style("color", "#B3B3B3")
    .style("font-size", "12px");

// Add Y axis label
svg.append("text")
    .attr("transform", "rotate(-90)")
    .attr("y", 0 - margin.left)
    .attr("x", 0 - (height / 2))
    .attr("dy", "1em")
    .style("text-anchor", "middle")
    .style("fill", "#B3B3B3")
    .style("font-size", "14px")
    .text("Duration (minutes)");

// Add X axis label
svg.append("text")
    .attr("x", width / 2)
    .attr("y", height + margin.bottom)
    .style("text-anchor", "middle")
    .style("fill", "#B3B3B3")
    .style("font-size", "14px")
    .text("Year");
})();