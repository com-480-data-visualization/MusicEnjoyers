(function () {
    // --- Constants ---
    var margin = { top: 64, right: 90, bottom: 58, left: 78 };
    var width = 760;
    var height = 400;
    var innerW = width - margin.left - margin.right;
    var innerH = height - margin.top - margin.bottom;
 
    // Format seconds as m:ss (e.g. 205 → "3:25"), matching the heatmap.
    function fmtMMSS(sec) {
        var s = Math.round(sec);
        var m = Math.floor(s / 60);
        var r = s % 60;
        return m + ":" + (r < 10 ? "0" : "") + r;
    }

    // --- Metric definitions ---
    var metrics = {
        duration: {
            label: "Duration",
            unit: "mm:ss",
            field: "avg_duration",
            yPad: 5,
            axisFormat: function (d) { return fmtMMSS(d); },
            format: function (d) { return fmtMMSS(d); },
            tooltipHtml: function (d) {
                return "<strong>" + d.year + "</strong><br>" +
                    "Avg: " + fmtMMSS(d.avg_duration) + "<br>" +
                    "Songs: " + d.count + "<br>" +
                    "Longest: " + d.max_track_duration  + " (" + fmtMMSS(d.max_duration) + ")<br>" +
                    "Shortest: " + d.min_track_duration +  " (" + fmtMMSS(d.min_duration) + ")";
            }
        },
        bpm: {
            label: "BPM",
            unit: "beats / min",
            field: "avg_bpm",
            yPad: 2,
            axisFormat: function (d) { return Math.round(d); },
            format: function (d) { return Math.round(d) + " bpm"; },
            tooltipHtml: function (d) {
                return "<strong>" + d.year + "</strong><br>" +
                    "Avg BPM: " + Math.round(d.avg_bpm) + "<br>" +
                    "Songs: " + d.count + "<br>" +
                    "Highest: " + d.max_track_bpm +  " (" + Math.round(d.max_bpm) + " bpm)<br>" +
                    "Lowest: " + d.min_track_bpm + " (" + Math.round(d.min_bpm) + " bpm)";
            }
        },
        loudness: {
            label: "Loudness",
            unit: "dB",
            field: "avg_loudness",
            yPad: 0.5,
            axisFormat: function (d) { return d.toFixed(1) + " dB"; },
            format: function (d) { return d.toFixed(1) + " dB"; },
            tooltipHtml: function (d) {
                return "<strong>" + d.year + "</strong><br>" +
                    "Avg loudness: " + d.avg_loudness.toFixed(2) + " dB<br>" +
                    "Songs: " + d.count + "<br>" +
                    "Loudest: " + d.max_track_loudness +  " (" + (+d.max_loudness).toFixed(1) + " dB)<br>" +
                    "Quietest: " + d.min_track_loudness +  " (" + (+d.min_loudness).toFixed(1) + " dB)";
            }
        },
        danceability: {
            label: "Danceability",
            unit: "0–100%",
            field: "avg_danceability",
            yPad: 0.02,
            axisFormat: function (d) { return (d * 100).toFixed(0) + "%"; },
            format: function (d) { return (d * 100).toFixed(1) + "%"; },
            tooltipHtml: function (d) {
                return "<strong>" + d.year + "</strong><br>" +
                    "Avg danceability: " + (d.avg_danceability * 100).toFixed(1) + "%<br>" +
                    "Songs: " + d.count + "<br>" +
                    "Most danceable: " + d.max_track_danceability +  " (" + (d.max_danceability * 100).toFixed(1) + "%)<br>" +
                    "Least danceable: " + d.min_track_danceability + " (" + (d.min_danceability * 100).toFixed(1) + "%)";
            }
        },
        speechiness: {
            label: "Speechiness",
            unit: "0–100%",
            field: "avg_speechiness",
            yPad: 0.005,
            axisFormat: function (d) { return (d * 100).toFixed(1) + "%"; },
            format: function (d) { return (d * 100).toFixed(1) + "%"; },
            tooltipHtml: function (d) {
                return "<strong>" + d.year + "</strong><br>" +
                    "Avg speechiness: " + (d.avg_speechiness * 100).toFixed(1) + "%<br>" +
                    "Songs: " + d.count + "<br>" +
                    "Most speech-like: " + d.max_track_speechiness + " (" + (d.max_speechiness * 100).toFixed(1) + "%)<br>" +
                    "Least speech-like: " + d.min_track_speechiness + " (" + (d.min_speechiness * 100).toFixed(1) + "%)";
            }
        },
        acousticness: {
            label: "Acousticness",
            unit: "0–100%",
            field: "avg_acousticness",
            yPad: 0.02,
            axisFormat: function (d) { return (d * 100).toFixed(0) + "%"; },
            format: function (d) { return (d * 100).toFixed(1) + "%"; },
            tooltipHtml: function (d) {
                return "<strong>" + d.year + "</strong><br>" +
                    "Avg acousticness: " + (d.avg_acousticness * 100).toFixed(1) + "%<br>" +
                    "Songs: " + d.count + "<br>" +
                    "Most acoustic: " + d.max_track_acousticness + " (" + (d.max_acousticness * 100).toFixed(1) + "%)<br>" +
                    "Least acoustic: " + d.min_track_acousticness +  " (" + (d.min_acousticness * 100).toFixed(1) + "%)";
            }
        },
        energy: {
            label: "Energy",
            unit: "0–100%",
            field: "avg_energy",
            yPad: 0.02,
            axisFormat: function (d) { return (d * 100).toFixed(0) + "%"; },
            format: function (d) { return (d * 100).toFixed(1) + "%"; },
            tooltipHtml: function (d) {
                return "<strong>" + d.year + "</strong><br>" +
                    "Avg energy: " + (d.avg_energy * 100).toFixed(1) + "%<br>" +
                    "Songs: " + d.count + "<br>" +
                    "Most energetic: " + d.max_track_energy + " (" + (d.max_energy * 100).toFixed(1) + "%)<br>" +
                    "Least energetic: " + d.min_track_energy + " (" + (d.min_energy * 100).toFixed(1) + "%)";
            }
        }
    };
 
    var activeMetric = "duration";
 
    // --- Tooltip (shared HTML element) ---
    var tooltip = d3.select("body")
        .append("div")
        .attr("class", "duration-tooltip");
 
    // --- Load data ---
    d3.json("data/general_stats.json").then(function (data) {
        data.forEach(function (d) {
            d.year              = +d.year;
            d.avg_duration      = +d.avg_duration;
            d.avg_loudness      = +d.avg_loudness;
            d.avg_bpm           = +d.avg_bpm;
            d.avg_energy        = +d.avg_energy;
            d.avg_danceability  = +d.avg_danceability;
            d.avg_speechiness   = +d.avg_speechiness;
            d.avg_acousticness  = +d.avg_acousticness;
            d.min_duration     = +d.min_duration;
            d.max_duration     = +d.max_duration;
            d.min_bpm          = +d.min_bpm;
            d.max_bpm          = +d.max_bpm;
            d.min_loudness     = +d.min_loudness;
            d.max_loudness     = +d.max_loudness;
            d.min_danceability = +d.min_danceability;
            d.max_danceability = +d.max_danceability;
            d.min_speechiness  = +d.min_speechiness;
            d.max_speechiness  = +d.max_speechiness;
            d.min_acousticness = +d.min_acousticness;
            d.max_acousticness = +d.max_acousticness;
            d.min_energy       = +d.min_energy;
            d.max_energy       = +d.max_energy;
        });
        // --- Big stat (initial render) ---
        updateStat(data, activeMetric);
 
        // --- SVG ---
        var svgRoot = d3.select("#dashboard")
            .append("svg")
            .attr("viewBox", "0 0 " + width + " " + height);

        var svg = svgRoot.append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        // --- Chart title + subtitle (centered over the full svg) ---
        var chartTitle = svgRoot.append("text")
            .attr("x", width / 2)
            .attr("y", 28)
            .attr("text-anchor", "middle")
            .attr("fill", "#FFFFFF")
            .style("font-size", "16px")
            .style("font-weight", "700")
            .style("letter-spacing", "0.02em");

        svgRoot.append("text")
            .attr("x", width / 2)
            .attr("y", 48)
            .attr("text-anchor", "middle")
            .attr("fill", "#B3B3B3")
            .style("font-size", "12px")
            .style("letter-spacing", "0.04em")
            .text("Billboard Hot 100 hits · 2016 – 2025");

        // --- Y axis title (rotated, in the left margin) ---
        var yAxisTitle = svg.append("text")
            .attr("transform", "rotate(-90)")
            .attr("x", -innerH / 2)
            .attr("y", -58)
            .attr("text-anchor", "middle")
            .attr("fill", "#B3B3B3")
            .style("font-size", "12px")
            .style("font-weight", "600")
            .style("letter-spacing", "0.05em");

        // --- X axis title (under the year labels) ---
        svg.append("text")
            .attr("x", innerW / 2)
            .attr("y", innerH + 44)
            .attr("text-anchor", "middle")
            .attr("fill", "#B3B3B3")
            .style("font-size", "12px")
            .style("font-weight", "600")
            .style("letter-spacing", "0.05em")
            .text("Year");
 
        // --- Scales ---
        var x = d3.scaleLinear()
            .domain([2016, 2025])
            .range([0, innerW]);
 
        var y = d3.scaleLinear()
            .range([innerH, 0]);
 
        // --- Axes (groups created once, updated on metric change) ---
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
 
        var yAxisG = svg.append("g");
 
        // --- Annotations (vertical dashed lines + labels) ---
        // Only meaningful for duration; hidden for other metrics
        // var annotations = [
        //     { year: 2016, label: "TikTok launches", dy: -20 },
        //     { year: 2020, label: "Instagram Reels", dy: -28 },
        //     { year: 2020, label: "YouTube Shorts",  dy:  28 }
        // ];
 
        // var annotGroup = svg.append("g").attr("class", "annotations");
 
        // --- Line path ---
        var linePath = svg.append("path")
            .attr("fill", "none")
            .attr("stroke", "#1DB954")
            .attr("stroke-width", 3);
 
        // --- Animation state ---
        var hasAnimated = false;
 
        // --- Data point circles ---
        var circles = svg.selectAll(".dot")
            .data(data)
            .enter()
            .append("circle")
            .attr("class", "dot")
            .attr("cx", function (d) { return x(d.year); })
            .attr("r", 5)
            .attr("fill", "#1DB954")
            .attr("stroke", "#121212")
            .attr("stroke-width", 2)
            .style("opacity", 0);
 
        // --- Endpoint labels group ---
        var labelsGroup = svg.append("g").attr("class", "endpoint-labels");
 
        // --- Tooltip overlay rect ---
        svg.append("rect")
            .attr("width", innerW)
            .attr("height", innerH)
            .attr("fill", "none")
            .attr("pointer-events", "all")
            .on("mousemove", function (event) {
                var pointer = d3.pointer(event);
                var xVal = x.invert(pointer[0]);
                var nearest = data.reduce(function (prev, curr) {
                    return Math.abs(curr.year - xVal) < Math.abs(prev.year - xVal) ? curr : prev;
                });
 
                circles
                    .attr("r", function (d) { return d.year === nearest.year ? 8 : 5; })
                    .style("opacity", function (d) {
                        return d.year === nearest.year ? 1 : (hasAnimated ? 0.7 : 0);
                    });
 
                tooltip
                    .html(metrics[activeMetric].tooltipHtml(nearest))
                    .classed("visible", true);
 
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
 
        // --- Render function ---
        function updateChart(metricKey, animate) {
            var m = metrics[metricKey];
            var field = m.field;

            // Titles
            chartTitle.text("Average " + m.label + " per year");
            yAxisTitle.text(m.label + (m.unit ? " (" + m.unit + ")" : ""));
 
            var yMin = d3.min(data, function (d) { return d[field]; });
            var yMax = d3.max(data, function (d) { return d[field]; });
            y.domain([yMin - m.yPad, yMax + m.yPad]);
 
            // Y axis
            yAxisG
                .call(
                    d3.axisLeft(y)
                        .ticks(5)
                        .tickFormat(m.axisFormat)
                )
                .call(function (g) { g.select(".domain").remove(); })
                .selectAll("text")
                .style("fill", "#B3B3B3")
                .style("font-size", "12px");
 
            svg.selectAll(".tick line").style("stroke", "#444");
 
 
            // Line
            var lineGen_db = d3.line()
                .defined(function(d) { return d[field] != null && !isNaN(d[field]); })
                .x(function (d) { return x(d.year); })
                .y(function (d) { return y(d[field]); })
                .curve(d3.curveMonotoneX);
 
            if (animate) {
                // Scroll-triggered first draw: use dash animation
                linePath.datum(data).attr("d", lineGen_db);
                var totalLength = linePath.node().getTotalLength();
                linePath
                    .attr("stroke-dasharray", totalLength)
                    .attr("stroke-dashoffset", totalLength)
                    .transition()
                    .duration(2000)
                    .ease(d3.easeCubicInOut)
                    .attr("stroke-dashoffset", 0);
            // } else {
            //     // Metric switch: smooth path tween
            //     linePath.datum(data)
            //         .transition()
            //         .duration(600)
            //         .ease(d3.easeCubicInOut)
            //         .attr("d", lineGen_db);
            // }
            } else {
                // Metric switch: clear dash animation first, then tween path
                linePath
                    .attr("stroke-dasharray", null)
                    .attr("stroke-dashoffset", null);
                linePath.datum(data)
                    .transition()
                    .duration(600)
                    .ease(d3.easeCubicInOut)
                    .attr("d", lineGen_db);
            }
            // Circles
            circles
                .transition()
                .duration(600)
                .attr("cy", function (d) { return y(d[field]); });
 
            // Endpoint labels
            labelsGroup.selectAll("*").remove();
            labelsGroup.append("text")
                .attr("x", x(data[0].year) + 10)
                .attr("y", y(data[0][field]))
                .attr("text-anchor", "start")
                .attr("dy", "0.0em")
                .attr("fill", "#FFFFFF")
                .attr("font-size", "13px")
                .attr("font-weight", 700)
                .text(m.format(data[0][field]));
            labelsGroup.append("text")
                .attr("x", x(data[data.length - 1].year) + 12)
                .attr("y", y(data[data.length - 1][field]))
                .attr("text-anchor", "start")
                .attr("dy", "0.35em")
                .attr("fill", "#FFFFFF")
                .attr("font-size", "13px")
                .attr("font-weight", 700)
                .text(m.format(data[data.length - 1][field]));
 
            // Big stat
            updateStat(data, metricKey);
        }
 

        var chartReady = false;
        document.getElementById('chart-knob-container').addEventListener('metric-change', function(e) {
            if (!chartReady) return;
            var key_db = e.detail.metric;
            if (key_db === activeMetric) return;
            activeMetric = key_db;
            updateChart(activeMetric, false);
        });
        // --- Initial render (no animation yet — wait for scroll) ---
        updateChart(activeMetric, false);
        chartReady = true; 
 
        // Set up line for scroll animation
        var lineGen0_db = d3.line()
            .x(function (d) { return x(d.year); })
            .y(function (d) { return y(d[metrics[activeMetric].field]); })
            .curve(d3.curveMonotoneX);
        linePath.datum(data).attr("d", lineGen0_db);
        var totalLength0_db = linePath.node().getTotalLength();
        linePath
            .attr("stroke-dasharray", totalLength0_db)
            .attr("stroke-dashoffset", totalLength0_db);
 
        // --- Scroll-triggered draw animation ---
        var observer_db = new IntersectionObserver(function (entries) {
            entries.forEach(function (entry) {
                if (entry.isIntersecting && !hasAnimated) {
                    hasAnimated = true;
 
                    // Draw the line
                    linePath.transition()
                        .duration(2000)
                        .ease(d3.easeCubicInOut)
                        .attr("stroke-dashoffset", 0);
 
                    // Stagger circles in
                    circles.transition()
                        .delay(function (_d, i) { return 200 + i * 150; })
                        .duration(400)
                        .style("opacity", 0.7);
                }
            });
        }, { threshold: 0.3 });
 
        observer_db.observe(document.getElementById("dashboard"));
    });
 
    // --- Big stat helper ---
    function updateStat(data, metricKey) {
        var m = metrics[metricKey];
        var field = m.field;
        var first = data[0][field];
        var last  = data[data.length - 1][field];
        var diff  = first - last;
        var sign  = diff > 0 ? "\u2212" : "+";
        var statEl = document.getElementById("dashboard-stat");
        if (!statEl) return;
        statEl.innerHTML =
            "<span>" + sign + m.format(Math.abs(diff)) + "</span>" +
            '<span class="stat-label">' + m.label.toLowerCase() + " change since " + data[0].year + "</span>";
    }
})();