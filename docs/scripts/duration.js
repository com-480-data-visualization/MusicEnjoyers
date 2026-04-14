(function () {
    // --- Constants ---
    var margin = { top: 40, right: 90, bottom: 50, left: 65 };
    var width = 760;
    var height = 400;
    var innerW = width - margin.left - margin.right;
    var innerH = height - margin.top - margin.bottom;

    // --- Tooltip (shared HTML element) ---
    var tooltip = d3.select("body")
        .append("div")
        .attr("class", "duration-tooltip");

    // --- Load data ---
    d3.json("data/duration_stats.json").then(function (data) {
        data.forEach(function (d) {
            d.year = +d.year;
            d.avg_seconds = +d.avg_seconds;
        });

        // --- Big stat ---
        var decrease = data[0].avg_seconds - data[data.length - 1].avg_seconds;
        var statEl = document.getElementById("duration-stat");
        statEl.innerHTML =
            "<span>\u2212" + Math.round(decrease) + "s</span>" +
            '<span class="stat-label">average duration lost since ' + data[0].year + "</span>";

        // --- SVG ---
        var svg = d3.select("#duration-line")
            .append("svg")
            .attr("viewBox", "0 0 " + width + " " + height)
            .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        // --- Scales ---
        var yMin = d3.min(data, function (d) { return d.avg_seconds; });
        var yMax = d3.max(data, function (d) { return d.avg_seconds; });
        var yPad = 5;

        var x = d3.scaleLinear()
            .domain([2016, 2025])
            .range([0, innerW]);

        var y = d3.scaleLinear()
            .domain([yMin - yPad, yMax + yPad])
            .range([innerH, 0]);

        // --- Axes ---
        svg.append("g")
            .attr("transform", "translate(0," + innerH + ")")
            .call(
                d3.axisBottom(x)
                    .ticks(10)
                    .tickFormat(d3.format("d"))
            )
            .call(function (g) { g.select(".domain").remove(); })
            .selectAll("text")
            .style("fill", "#B3B3B3")
            .style("font-size", "12px");

        svg.append("g")
            .call(
                d3.axisLeft(y)
                    .ticks(5)
                    .tickFormat(function (d) { return Math.round(d) + "s"; })
            )
            .call(function (g) { g.select(".domain").remove(); })
            .selectAll("text")
            .style("fill", "#B3B3B3")
            .style("font-size", "12px");

        // Style tick lines
        svg.selectAll(".tick line").style("stroke", "#444");

        // --- Annotations (vertical dashed lines + labels) ---
        var annotations = [
            { year: 2016, label: "TikTok launches", dy: -20 },
            { year: 2020, label: "Instagram Reels", dy: -28 },
            { year: 2020, label: "YouTube Shorts", dy: 28 }
        ];

        var annotGroup = svg.append("g").attr("class", "annotations");

        annotations.forEach(function (a) {
            var dataPoint = data.find(function (d) { return d.year === a.year; });
            if (!dataPoint) return;

            var cx = x(a.year);
            var cy = y(dataPoint.avg_seconds);

            // Dashed vertical line from axis to data point
            annotGroup.append("line")
                .attr("x1", cx).attr("x2", cx)
                .attr("y1", innerH).attr("y2", cy)
                .attr("stroke", "#555")
                .attr("stroke-dasharray", "4,4")
                .attr("stroke-width", 1);

            // Label
            annotGroup.append("text")
                .attr("x", cx)
                .attr("y", cy + a.dy)
                .attr("text-anchor", "middle")
                .attr("fill", "#B3B3B3")
                .attr("font-size", "11px")
                .text(a.label);
        });

        // --- Line path ---
        var line = d3.line()
            .x(function (d) { return x(d.year); })
            .y(function (d) { return y(d.avg_seconds); })
            .curve(d3.curveMonotoneX);

        var path = svg.append("path")
            .datum(data)
            .attr("fill", "none")
            .attr("stroke", "#1DB954")
            .attr("stroke-width", 3)
            .attr("d", line);

        var totalLength = path.node().getTotalLength();
        path.attr("stroke-dasharray", totalLength)
            .attr("stroke-dashoffset", totalLength);

        // --- Animation state (declared early for tooltip handlers) ---
        var hasAnimated = false;

        // --- Data point circles ---
        var circles = svg.selectAll(".dot")
            .data(data)
            .enter()
            .append("circle")
            .attr("class", "dot")
            .attr("cx", function (d) { return x(d.year); })
            .attr("cy", function (d) { return y(d.avg_seconds); })
            .attr("r", 5)
            .attr("fill", "#1DB954")
            .attr("stroke", "#121212")
            .attr("stroke-width", 2)
            .style("opacity", 0);

        // --- Direct labels on endpoints ---
        function formatDuration(sec) {
            var m = Math.floor(sec / 60);
            var s = Math.round(sec % 60);
            return m + "m " + (s < 10 ? "0" : "") + s + "s";
        }

        // First point label (right of dot, vertically centered)
        svg.append("text")
            .attr("x", x(data[0].year) + 10)
            .attr("y", y(data[0].avg_seconds))
            .attr("text-anchor", "start")
            .attr("dy", "0.0em")
            .attr("fill", "#FFFFFF")
            .attr("font-size", "13px")
            .attr("font-weight", 700)
            .text(formatDuration(data[0].avg_seconds));

        // Last point label (right side)
        svg.append("text")
            .attr("x", x(data[data.length - 1].year) + 12)
            .attr("y", y(data[data.length - 1].avg_seconds))
            .attr("text-anchor", "start")
            .attr("dy", "0.35em")
            .attr("fill", "#FFFFFF")
            .attr("font-size", "13px")
            .attr("font-weight", 700)
            .text(formatDuration(data[data.length - 1].avg_seconds));

        // --- Tooltip interaction ---
        // Invisible overlay rect to capture mouse events
        svg.append("rect")
            .attr("width", innerW)
            .attr("height", innerH)
            .attr("fill", "none")
            .attr("pointer-events", "all")
            .on("mousemove", function (event) {
                var pointer = d3.pointer(event);
                var xVal = x.invert(pointer[0]);
                // Snap to nearest year
                var nearest = data.reduce(function (prev, curr) {
                    return Math.abs(curr.year - xVal) < Math.abs(prev.year - xVal) ? curr : prev;
                });

                // Highlight circle
                circles
                    .attr("r", function (d) { return d.year === nearest.year ? 8 : 5; })
                    .style("opacity", function (d) { return d.year === nearest.year ? 1 : (hasAnimated ? 0.7 : 0); });

                // Tooltip content
                var mins = Math.floor(nearest.avg_seconds / 60);
                var secs = Math.round(nearest.avg_seconds % 60);
                var html =
                    "<strong>" + nearest.year + "</strong><br>" +
                    "Avg: " + mins + "m " + (secs < 10 ? "0" : "") + secs + "s (" + nearest.avg_seconds + "s)<br>" +
                    "Songs: " + nearest.count + "<br>" +
                    "Longest: " + nearest.max_track + " (" + Math.round(nearest.max_seconds) + "s)<br>" +
                    "Shortest: " + nearest.min_track + " (" + Math.round(nearest.min_seconds) + "s)";

                tooltip.html(html).classed("visible", true);

                // Position tooltip
                var ttNode = tooltip.node();
                var ttW = ttNode.offsetWidth;
                var px = event.pageX + 15;
                if (px + ttW > window.innerWidth - 20) {
                    px = event.pageX - ttW - 15;
                }
                tooltip
                    .style("left", px + "px")
                    .style("top", (event.pageY - 20) + "px");
            })
            .on("mouseleave", function () {
                tooltip.classed("visible", false);
                circles
                    .attr("r", 5)
                    .style("opacity", hasAnimated ? 0.7 : 0);
            });

        // --- Scroll-triggered draw animation ---
        var observer = new IntersectionObserver(function (entries) {
            entries.forEach(function (entry) {
                if (entry.isIntersecting && !hasAnimated) {
                    hasAnimated = true;

                    // Draw the line
                    path.transition()
                        .duration(2000)
                        .ease(d3.easeCubicInOut)
                        .attr("stroke-dashoffset", 0);

                    // Stagger circles
                    circles.transition()
                        .delay(function (_d, i) { return 200 + i * 150; })
                        .duration(400)
                        .style("opacity", 0.7);
                }
            });
        }, { threshold: 0.3 });

        observer.observe(document.getElementById("duration-line"));
    });
})();
