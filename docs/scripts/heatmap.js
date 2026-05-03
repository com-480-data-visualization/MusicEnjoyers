(function () {

    // --- Constants ---
    var margin = { top: 50, right: 170, bottom: 55, left: 110 };
    var width  = 860;
    var height = 460;
    var innerW = width  - margin.left - margin.right;
    var innerH = height - margin.top  - margin.bottom;

    var years = d3.range(2016, 2026); // 2016–2025 inclusive

    // --- Metric definitions ---
    var metrics = [
        {
            key:   "duration",
            label: "Duration",
            // bins are in seconds
            formatBinEdge: function (s) {
                var m = Math.floor(s / 60), sec = s % 60;
                return m + ":" + (sec < 10 ? "0" : "") + sec;
            },
            formatRange: function (lo, hi) {
                var fmt = function (s) {
                    var m = Math.floor(s / 60), sec = s % 60;
                    return m + ":" + (sec < 10 ? "0" : "") + sec;
                };
                return fmt(lo) + " – " + fmt(hi);
            }
        },
        {
            key:   "bpm",
            label: "BPM",
            formatBinEdge: function (v) { return Math.round(v); },
            formatRange:   function (lo, hi) { return lo + " – " + hi + " bpm"; }
        },
        {
            key:   "loudness",
            label: "Loudness",
            formatBinEdge: function (v) { return v.toFixed(0) + " dB"; },
            formatRange:   function (lo, hi) { return lo + " to " + hi + " dB"; }
        },
        {
            key:   "danceability",
            label: "Danceability",
            formatBinEdge: function (v) { return Math.round(v * 100) + "%"; },
            formatRange:   function (lo, hi) {
                return Math.round(lo * 100) + "–" + Math.round(hi * 100) + "%";
            }
        },
        {
            key:   "speechiness",
            label: "Speechiness",
            formatBinEdge: function (v) { return Math.round(v * 100) + "%"; },
            formatRange:   function (lo, hi) {
                return Math.round(lo * 100) + "–" + Math.round(hi * 100) + "%";
            }
        },
        {
            key:   "acousticness",
            label: "Acousticness",
            formatBinEdge: function (v) { return Math.round(v * 100) + "%"; },
            formatRange:   function (lo, hi) {
                return Math.round(lo * 100) + "–" + Math.round(hi * 100) + "%";
            }
        },
        {
            key:   "energy",
            label: "Energy",
            formatBinEdge: function (v) { return Math.round(v * 100) + "%"; },
            formatRange:   function (lo, hi) {
                return Math.round(lo * 100) + "–" + Math.round(hi * 100) + "%";
            }
        }
    ];

    var activeMetricIdx = 0;

    // --- Tooltip ---
    var tooltip = d3.select("body")
        .append("div")
        .attr("class", "heatmap-tooltip");

    // --- Load data ---
    // Expected JSON shape (duration in seconds):
    // {
    //   "duration": {
    //     "bins": [60, 120, 150, 180, 210, 240, 300],  // n+1 edges for n bins
    //     "years": {
    //       "2016": [3, 12, 45, 67, 22, 4],            // count per bin, low→high
    //       "2017": [...]
    //     }
    //   },
    //   "bpm": { "bins": [...], "years": { ... } },
    //   ...
    // }
    d3.json("data/heatmap_data.json").then(function (data) {

        // --- SVG ---
        var svg = d3.select("#heatmap")
            .append("svg")
            .attr("viewBox", "0 0 " + width + " " + height)
            .attr("preserveAspectRatio", "xMidYMid meet");

        var g = svg.append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        var cellW = innerW / years.length;

        // --- X axis (fixed, drawn once) ---
        var xScale = d3.scaleLinear()
            .domain([2016, 2025])
            .range([cellW / 2, innerW - cellW / 2]);

        g.append("g")
            .attr("class", "hm-xaxis")
            .attr("transform", "translate(0," + (innerH + 14) + ")")
            .call(
                d3.axisBottom(xScale)
                    .ticks(10)
                    .tickFormat(d3.format("d"))
                    .tickSize(0)
            )
            .call(function (ax) { ax.select(".domain").remove(); })
            .selectAll("text")
            .style("fill", "#B3B3B3")
            .style("font-size", "12px");

        // --- Y axis group (rebuilt per metric) ---
        var yAxisG = g.append("g").attr("class", "hm-yaxis");

        // --- Cells group (rebuilt per metric) ---
        var cellsG = g.append("g").attr("class", "hm-cells");

        // --- Slider ---
        var sliderX   = innerW + 32;
        var trackH    = innerH;
        var stepH     = trackH / (metrics.length - 1);

        sliderG_setup();

        function sliderG_setup() {
            var sliderG = g.append("g").attr("class", "hm-slider");

            // Track line
            sliderG.append("line")
                .attr("x1", sliderX + 8).attr("y1", 0)
                .attr("x2", sliderX + 8).attr("y2", trackH)
                .attr("stroke", "#333")
                .attr("stroke-width", 2)
                .attr("stroke-linecap", "round");

            // Knob
            var knob = sliderG.append("circle")
                .attr("class", "hm-knob")
                .attr("cx", sliderX + 8)
                .attr("cy", idxToKnobY(activeMetricIdx))
                .attr("r", 10)
                .attr("fill", "#1DB954")
                .attr("stroke", "#121212")
                .attr("stroke-width", 2);

            // Per-metric tick marks + labels
            metrics.forEach(function (m, i) {
                var cy = idxToKnobY(i);

                sliderG.append("line")
                    .attr("x1", sliderX + 2).attr("y1", cy)
                    .attr("x2", sliderX + 14).attr("y2", cy)
                    .attr("stroke", "#444").attr("stroke-width", 1);

                sliderG.append("text")
                    .attr("class", "hm-slider-label")
                    .attr("data-idx", i)
                    .attr("x", sliderX + 24)
                    .attr("y", cy)
                    .attr("dominant-baseline", "middle")
                    .attr("fill",       i === 0 ? "#1DB954" : "#B3B3B3")
                    .attr("font-weight", i === 0 ? "700"    : "400")
                    .attr("font-size", "11px")
                    .style("cursor", "pointer")
                    .text(m.label)
                    .on("click", function () { switchMetric(i); });

                // Wide invisible click zone
                sliderG.append("rect")
                    .attr("x", sliderX)
                    .attr("y", cy - stepH / 2)
                    .attr("width", 115)
                    .attr("height", stepH)
                    .attr("fill", "none")
                    .attr("pointer-events", "all")
                    .style("cursor", "pointer")
                    .on("click", function () { switchMetric(i); });
            });

            // Drag
            var drag = d3.drag()
                .on("drag", function (event) {
                    var clampedY = Math.max(0, Math.min(trackH, event.y));
                    knob.attr("cy", clampedY);
                    var nearest = Math.max(0, Math.min(
                        metrics.length - 1,
                        Math.round(clampedY / stepH)
                    ));
                    if (nearest !== activeMetricIdx) switchMetric(nearest);
                })
                .on("end", function () {
                    knob.transition().duration(200)
                        .attr("cy", idxToKnobY(activeMetricIdx));
                });

            knob.call(drag);

            // Expose knob + labels so switchMetric can update them
            g.property("_knob", knob);
        }

        function idxToKnobY(idx) {
            return idx * stepH;
        }

        // --- Color scale (rebuilt per metric using that metric's max count) ---
        var colorScale = d3.scaleSequential()
            .interpolator(d3.interpolate("#0d1f12", "#1DB954"));

        // --- Render ---
        function renderMetric(idx, animate) {
            var m    = metrics[idx];
            var mDat = data[m.key];
            if (!mDat) { console.warn("No heatmap data for:", m.key); return; }

            var bins  = mDat.bins;        // n+1 edge values
            var nBins = bins.length - 1;
            var cellH = innerH / nBins;

            // Max count across all years for this metric
            var allCounts = [];
            years.forEach(function (yr) {
                var row = mDat.years[String(yr)];
                if (row) row.forEach(function (c) { allCounts.push(c); });
            });
            colorScale.domain([0, d3.max(allCounts) || 1]);

            // Flat cell array
            var cells = [];
            years.forEach(function (yr, col) {
                var row   = mDat.years[String(yr)] || [];
                var total = data.totals ? (data.totals[String(yr)] || 0) : 0;
                for (var b = 0; b < nBins; b++) {
                    cells.push({
                        year:  yr,
                        col:   col,
                        bin:   b,          // 0 = lowest value bucket
                        count: row[b] || 0,
                        total: total,
                        lo:    bins[b],
                        hi:    bins[b + 1]
                    });
                }
            });

            // Y scale: bin 0 at BOTTOM (low values at bottom, high at top)
            var yScale = d3.scaleBand()
                .domain(d3.range(nBins - 1, -1, -1))  // reversed so bin 0 = bottom
                .range([0, innerH])
                .padding(0.04);

            // Y axis labels (bin edges)
            yAxisG.selectAll("*").remove();
            var labelEvery = nBins > 12 ? 2 : 1;
            // Label each displayed edge
            for (var b = 0; b <= nBins; b += labelEvery) {
                // pixel position: edge b is between bin (b-1) and bin b
                var yPos;
                if (b === 0) {
                    yPos = innerH; // bottom edge = lowest value
                } else if (b === nBins) {
                    yPos = 0;     // top edge = highest value
                } else {
                    // top of the band for bin (nBins-1-b+1)... simpler: just lerp
                    yPos = innerH - (b / nBins) * innerH;
                }
                yAxisG.append("text")
                    .attr("x", -10)
                    .attr("y", yPos)
                    .attr("text-anchor", "end")
                    .attr("dominant-baseline", "middle")
                    .attr("fill", "#666")
                    .attr("font-size", "10px")
                    .text(m.formatBinEdge(bins[b]));
            }

            // Draw cells
            cellsG.selectAll("*").remove();

            cellsG.selectAll(".hm-cell")
                .data(cells)
                .enter()
                .append("rect")
                .attr("class", "hm-cell")
                .attr("x",      function (d) { return d.col * cellW + 1; })
                .attr("y",      function (d) { return yScale(d.bin); })
                .attr("width",  cellW - 2)
                .attr("height", yScale.bandwidth())
                .attr("rx", 2)
                .attr("fill",   function (d) {
                    return d.count === 0 ? "#111" : colorScale(d.count);
                })
                .attr("opacity", animate ? 0 : 1)
                .each(function (d) {
                    var cellData   = d;
                    var metricMeta = m;
                    var self       = this;
                    self.addEventListener("mouseover", function (event) {
                        d3.select(self).attr("stroke", "#fff").attr("stroke-width", 1.5);
                        tooltip.html(buildTooltip(metricMeta, cellData)).classed("visible", true);
                        positionTooltip(event);
                    });
                    self.addEventListener("mousemove", function (event) {
                        positionTooltip(event);
                    });
                    self.addEventListener("mouseout", function () {
                        d3.select(self).attr("stroke", null).attr("stroke-width", null);
                        tooltip.classed("visible", false);
                    });
                });

            var rects = cellsG.selectAll(".hm-cell");

            if (animate) {
                rects.transition()
                    .delay(function (d) { return d.col * 55 + (nBins - 1 - d.bin) * 8; })
                    .duration(350)
                    .attr("opacity", 1);
            }
        }

        function positionTooltip(event) {
            var ttNode = tooltip.node();
            var ttW    = ttNode.offsetWidth;
            var ttH    = ttNode.offsetHeight;
            var px     = event.clientX + 15;
            var py     = event.clientY - ttH / 2;
            if (px + ttW > window.innerWidth - 20)  px = event.clientX - ttW - 15;
            if (py < 10)                             py = 10;
            if (py + ttH > window.innerHeight - 10) py = window.innerHeight - ttH - 10;
            tooltip
                .style("left", px + "px")
                .style("top",  py + "px");
        }

        function buildTooltip(m, d) {
            var countLine;
            if (d.count === 0) {
                countLine = "<span class='tt-zero'>no songs</span>";
            } else if (d.total > 0) {
                var pct = (d.count / d.total * 100).toFixed(1);
                countLine = "<span class='tt-count'>" +
                    "<span class='tt-count-pct'>" + pct + "%</span>" +
                    "<span class='tt-count-abs'>" + d.count + " / " + d.total + "</span>" +
                "</span>";
            } else {
                countLine = "<span class='tt-count'>" + d.count + " songs</span>";
            }
            return "<span class='tt-year'>"   + d.year   + "</span>" +
                   "<span class='tt-metric'>" + m.label  + "</span>" +
                   "<span class='tt-range'>"  + m.formatRange(d.lo, d.hi) + "</span>" +
                   countLine;
        }

        // --- Metric switch ---
        function switchMetric(idx) {
            if (idx === activeMetricIdx) return;
            activeMetricIdx = idx;

            // Animate knob
            g.property("_knob")
                .transition().duration(350).ease(d3.easeCubicInOut)
                .attr("cy", idxToKnobY(idx));

            // Update slider labels
            g.selectAll(".hm-slider-label")
                .attr("fill",        function () {
                    return +d3.select(this).attr("data-idx") === idx ? "#1DB954" : "#B3B3B3";
                })
                .attr("font-weight", function () {
                    return +d3.select(this).attr("data-idx") === idx ? "700" : "400";
                });

            renderMetric(idx, false);

            // Keep dashboard.js line chart in sync
            var knobContainer = document.getElementById("chart-knob-container");
            if (knobContainer) {
                knobContainer.dispatchEvent(new CustomEvent("metric-change", {
                    detail: { metric: metrics[idx].key }
                }));
            }
        }

        // Sync FROM dashboard.js → heatmap
        var keyToIdx = {};
        metrics.forEach(function (m, i) { keyToIdx[m.key] = i; });

        var knobContainer = document.getElementById("chart-knob-container");
        if (knobContainer) {
            knobContainer.addEventListener("metric-change", function (e) {
                var key = e.detail && e.detail.metric;
                if (key != null && keyToIdx[key] != null && keyToIdx[key] !== activeMetricIdx) {
                    activeMetricIdx = keyToIdx[key];
                    g.property("_knob")
                        .transition().duration(350).ease(d3.easeCubicInOut)
                        .attr("cy", idxToKnobY(activeMetricIdx));
                    g.selectAll(".hm-slider-label")
                        .attr("fill",        function () {
                            return +d3.select(this).attr("data-idx") === activeMetricIdx ? "#1DB954" : "#B3B3B3";
                        })
                        .attr("font-weight", function () {
                            return +d3.select(this).attr("data-idx") === activeMetricIdx ? "700" : "400";
                        });
                    renderMetric(activeMetricIdx, false);
                }
            });
        }

        // --- Scroll-triggered animation ---
        var hasAnimated = false;
        var observer = new IntersectionObserver(function (entries) {
            entries.forEach(function (entry) {
                if (entry.isIntersecting && !hasAnimated) {
                    hasAnimated = true;
                    renderMetric(activeMetricIdx, true);
                }
            });
        }, { threshold: 0.2 });

        var el = document.getElementById("heatmap");
        if (el) observer.observe(el);

        // Silent initial render
        renderMetric(activeMetricIdx, false);
    });

})();