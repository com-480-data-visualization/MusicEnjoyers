(function () {

    // --- Constants ---
    var margin = { top: 65, right: 170, bottom: 110, left: 130 };
    var width  = 860;
    var height = 540;
    var innerW = width  - margin.left - margin.right;
    var innerH = height - margin.top  - margin.bottom;

    var years = d3.range(2016, 2026); // 2016–2025 inclusive

    // --- Metric definitions ---
    var metrics = [
        {
            key:   "duration",
            label: "Duration",
            unit:  "mm:ss",
            description:
                "<strong>Duration</strong> is simply how long a track runs. Hits have been getting shorter as streaming and short-video platforms reward songs that reach the hook within the first few seconds, before a listener can skip.",
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
            unit:  "BPM",
            description:
                "<strong>BPM</strong> (beats per minute) is a track's tempo, the speed of its underlying pulse. Higher values feel faster and more urgent; lower values feel relaxed. Many recent hits cluster around danceable mid-tempos.",
            formatBinEdge: function (v) { return Math.round(v); },
            formatRange:   function (lo, hi) { return lo + " – " + hi + " bpm"; }
        },
        {
            key:   "loudness",
            label: "Loudness",
            unit:  "dB",
            description:
                "<strong>Decibels (dB)</strong> measure loudness <strong>relative to the digital ceiling (0 dB)</strong>, the loudest a recording can possibly be without clipping. Real audio always sits below this ceiling, so the values are negative, and the closer to 0, the louder. A modern pop hit typically lands around <strong>-5 dB</strong>; a quiet acoustic track might be <strong>-20 dB</strong> or lower. The upward drift over the years is the so-called <em>loudness war</em>, labels mastering hits hotter to cut through phone speakers and streaming playlists.",
            formatBinEdge: function (v) { return v.toFixed(0) + " dB"; },
            formatRange:   function (lo, hi) { return lo + " to " + hi + " dB"; }
        },
        {
            key:   "danceability",
            label: "Danceability",
            unit:  "0–100",
            description:
                "<strong>Danceability</strong> describes how suitable a track is for dancing, combining tempo, rhythm stability, beat strength and overall regularity into a single 0–100 score. Higher means a steadier, more body-moving groove.",
            formatBinEdge: function (v) { return Math.round(v * 100) + "%"; },
            formatRange:   function (lo, hi) {
                return Math.round(lo * 100) + "–" + Math.round(hi * 100) + "%";
            }
        },
        {
            key:   "speechiness",
            label: "Speechiness",
            unit:  "0–100",
            description:
                "<strong>Speechiness</strong> detects the presence of spoken words. Rap verses, spoken intros and talk-heavy tracks push the value up, while purely sung or instrumental music stays low. It's a good proxy for how rap-driven the charts are.",
            formatBinEdge: function (v) { return Math.round(v * 100) + "%"; },
            formatRange:   function (lo, hi) {
                return Math.round(lo * 100) + "–" + Math.round(hi * 100) + "%";
            }
        },
        {
            key:   "acousticness",
            label: "Acousticness",
            unit:  "0–100",
            description:
                "<strong>Acousticness</strong> is a 0–100 confidence that a track is acoustic, made with real instruments rather than electronic production. High values point to stripped-back, unplugged recordings; low values to heavily produced, electronic sound.",
            formatBinEdge: function (v) { return Math.round(v * 100) + "%"; },
            formatRange:   function (lo, hi) {
                return Math.round(lo * 100) + "–" + Math.round(hi * 100) + "%";
            }
        },
        {
            key:   "energy",
            label: "Energy",
            unit:  "0–100",
            description:
                "<strong>Energy</strong> is a 0–100 measure of intensity and activity. Fast, loud and noisy tracks (think hard rock or EDM) score high; calm, mellow acoustic ballads sit at the low end.",
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

        // --- Per-metric caption (HTML, below the chart) ---
        var captionEl = document.getElementById("heatmap-caption");

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

        // --- Trend line group (drawn on top of cells; dots stay interactive,
        //     line + label opt out of pointer events to keep cells hoverable) ---
        var trendG = g.append("g").attr("class", "hm-trend");

        // --- Chart title (top, centered on full svg) ---
        var chartTitle = svg.append("text")
            .attr("class", "hm-chart-title")
            .attr("x", width / 2)
            .attr("y", 30)
            .attr("text-anchor", "middle")
            .attr("fill", "#FFFFFF")
            .style("font-size", "16px")
            .style("font-weight", "700")
            .style("letter-spacing", "0.02em");

        // --- Y axis title (rotated, in left margin) ---
        var yAxisTitle = g.append("text")
            .attr("class", "hm-yaxis-title")
            .attr("transform", "rotate(-90)")
            .attr("x", -innerH / 2)
            .attr("y", -85)
            .attr("text-anchor", "middle")
            .attr("fill", "#B3B3B3")
            .style("font-size", "12px")
            .style("font-weight", "600")
            .style("letter-spacing", "0.05em");

        // --- X axis title (under year labels) ---
        g.append("text")
            .attr("class", "hm-xaxis-title")
            .attr("x", innerW / 2)
            .attr("y", innerH + 40)
            .attr("text-anchor", "middle")
            .attr("fill", "#B3B3B3")
            .style("font-size", "12px")
            .style("font-weight", "600")
            .style("letter-spacing", "0.05em")
            .text("Year");

        // --- Color legend (gradient + 0% / max% ticks) ---
        var defs   = svg.append("defs");
        var nStops = 11;
        var gradient = defs.append("linearGradient")
            .attr("id", "hm-color-grad")
            .attr("x1", "0%").attr("x2", "100%")
            .attr("y1", "0%").attr("y2", "0%");
        for (var s = 0; s < nStops; s++) {
            gradient.append("stop")
                .attr("class", "hm-grad-stop")
                .attr("offset", (s / (nStops - 1) * 100) + "%");
        }

        var legendW = 200, legendH = 10;
        var legendX = (innerW - legendW) / 2;
        var legendY = innerH + 65;
        var legendG = g.append("g").attr("class", "hm-legend");

        legendG.append("text")
            .attr("x", legendX + legendW / 2)
            .attr("y", legendY - 6)
            .attr("text-anchor", "middle")
            .attr("fill", "#B3B3B3")
            .style("font-size", "10px")
            .style("font-weight", "600")
            .style("letter-spacing", "0.05em")
            .text("Share of yearly hits");

        legendG.append("rect")
            .attr("x", legendX).attr("y", legendY)
            .attr("width", legendW).attr("height", legendH)
            .attr("fill", "url(#hm-color-grad)")
            .attr("stroke", "#333")
            .attr("stroke-width", 0.5)
            .attr("rx", 1);

        legendG.append("text")
            .attr("x", legendX)
            .attr("y", legendY + legendH + 12)
            .attr("text-anchor", "start")
            .attr("fill", "#B3B3B3")
            .style("font-size", "10px")
            .text("0%");

        var legendMaxLabel = legendG.append("text")
            .attr("class", "hm-legend-max")
            .attr("x", legendX + legendW)
            .attr("y", legendY + legendH + 12)
            .attr("text-anchor", "end")
            .attr("fill", "#B3B3B3")
            .style("font-size", "10px");

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

        // --- Color scale (rebuilt per metric using that metric's max % share) ---
        var colorScale = d3.scaleSequential()
            .interpolator(d3.interpolate("#0d1f12", "#1DB954"));

        // --- Weighted median over a binned distribution ---
        // row[b] = count in bin b; bins[b]/bins[b+1] = numeric bin edges.
        // Returns { f, value } where f is the position in [0,1] from low edge to
        // high edge across the full bin range, and value is in raw metric units.
        // Format an absolute median value with the metric's natural unit.
        function fmtMedianValue(m, v) {
            if (m.key === "duration") {
                var s  = Math.round(v);
                var mm = Math.floor(s / 60);
                var ss = s % 60;
                return mm + ":" + (ss < 10 ? "0" : "") + ss;
            }
            if (m.key === "loudness") return v.toFixed(1) + " dB";
            if (m.key === "bpm")      return Math.round(v) + " BPM";
            return Math.round(v * 100) + "%";   // 0–1 features
        }

        // Format a signed delta (curr − baseline) with the metric's unit.
        function fmtMedianDelta(m, deltaVal) {
            if (deltaVal === 0) return "no change";
            var sign = deltaVal > 0 ? "+" : "−";
            var abs  = Math.abs(deltaVal);
            if (m.key === "duration") {
                var s  = Math.round(abs);
                if (s < 60) return sign + s + "s";
                var mm = Math.floor(s / 60);
                var ss = s % 60;
                return sign + mm + ":" + (ss < 10 ? "0" : "") + ss;
            }
            if (m.key === "loudness") return sign + abs.toFixed(1) + " dB";
            if (m.key === "bpm")      return sign + abs.toFixed(1) + " BPM";
            return sign + (abs * 100).toFixed(1) + " pp";  // percentage points
        }

        function computeWeightedMedian(row, bins, nBins) {
            var total = 0;
            for (var i = 0; i < nBins; i++) total += (row[i] || 0);
            if (total === 0) return null;
            var half = total / 2;
            var cum  = 0;
            for (var b = 0; b < nBins; b++) {
                var ct   = row[b] || 0;
                var next = cum + ct;
                if (next >= half && ct > 0) {
                    var fWithin = (half - cum) / ct;
                    return {
                        f:     (b + fWithin) / nBins,
                        value: bins[b] + fWithin * (bins[b + 1] - bins[b])
                    };
                }
                cum = next;
            }
            return null;
        }

        // --- Render ---
        function renderMetric(idx, animate) {
            var m    = metrics[idx];
            var mDat = data[m.key];
            if (!mDat) { console.warn("No heatmap data for:", m.key); return; }

            var bins  = mDat.bins;        // n+1 edge values
            var nBins = bins.length - 1;

            // Flat cell array, encoding share-of-yearly-hits (% of total)
            var cells   = [];
            var allPcts = [];
            years.forEach(function (yr, col) {
                var row   = mDat.years[String(yr)] || [];
                var total = data.totals ? (data.totals[String(yr)] || 0) : 0;
                for (var b = 0; b < nBins; b++) {
                    var ct  = row[b] || 0;
                    var pct = total > 0 ? (ct / total) * 100 : 0;
                    cells.push({
                        year:  yr,
                        col:   col,
                        bin:   b,          // 0 = lowest value bucket
                        count: ct,
                        total: total,
                        pct:   pct,
                        lo:    bins[b],
                        hi:    bins[b + 1]
                    });
                    if (ct > 0) allPcts.push(pct);
                }
            });

            // Color scale on % share, not raw count: years have unequal totals
            var maxPct = d3.max(allPcts) || 1;
            colorScale.domain([0, maxPct]);

            // Update legend gradient stops + max-tick label
            defs.select("#hm-color-grad").selectAll(".hm-grad-stop")
                .attr("stop-color", function (_, i) {
                    return colorScale(maxPct * (i / (nStops - 1)));
                });
            legendMaxLabel.text(maxPct.toFixed(1) + "%");

            // Update titles
            chartTitle.text("Distribution of " + m.label + " in Billboard Hot 100 hits, 2016–2025");
            yAxisTitle.text(m.label + (m.unit ? " (" + m.unit + ")" : ""));

            // Per-metric caption: only shown when a description exists
            if (captionEl) {
                if (m.description) {
                    captionEl.innerHTML =
                        "<span class='hm-caption-label'>About " + m.label + "</span>" +
                        m.description;
                    captionEl.hidden = false;
                } else {
                    captionEl.innerHTML = "";
                    captionEl.hidden = true;
                }
            }

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
                    return d.count === 0 ? "#111" : colorScale(d.pct);
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

            // --- Median trend line (weighted median over bins, per year) ---
            trendG.selectAll("*").remove();

            var medianPts = [];
            years.forEach(function (yr, col) {
                var row = mDat.years[String(yr)] || [];
                var med = computeWeightedMedian(row, bins, nBins);
                if (med != null) {
                    medianPts.push({
                        year:  yr,
                        x:     col * cellW + cellW / 2,
                        y:     innerH * (1 - med.f),
                        value: med.value
                    });
                }
            });

            if (medianPts.length >= 2) {
                var lineGen = d3.line()
                    .x(function (d) { return d.x; })
                    .y(function (d) { return d.y; })
                    .curve(d3.curveMonotoneX);

                var trendPath = trendG.append("path")
                    .datum(medianPts)
                    .attr("fill", "none")
                    .attr("stroke", "#FFFFFF")
                    .attr("stroke-width", 2)
                    .attr("stroke-opacity", 0.95)
                    .attr("stroke-linecap", "round")
                    .attr("stroke-linejoin", "round")
                    .style("pointer-events", "none")
                    .attr("d", lineGen);

                var trendDots = trendG.selectAll(".hm-median-dot")
                    .data(medianPts)
                    .enter()
                    .append("circle")
                    .attr("class", "hm-median-dot")
                    .attr("cx", function (d) { return d.x; })
                    .attr("cy", function (d) { return d.y; })
                    .attr("r", 3)
                    .attr("fill", "#FFFFFF")
                    .attr("stroke", "#121212")
                    .attr("stroke-width", 1)
                    .style("pointer-events", "none");

                // "median" label at the right end
                var lastPt = medianPts[medianPts.length - 1];
                var trendLabel = trendG.append("text")
                    .attr("x", lastPt.x + 10)
                    .attr("y", lastPt.y)
                    .attr("dominant-baseline", "middle")
                    .attr("fill", "#FFFFFF")
                    .style("font-size", "10px")
                    .style("font-weight", "700")
                    .style("letter-spacing", "0.05em")
                    .style("pointer-events", "none")
                    .text("median");

                // Invisible larger hit-targets so the small dots are easy to hover.
                var baseline = medianPts[0];
                var trendDotNodes = trendDots.nodes();
                trendG.selectAll(".hm-median-hit")
                    .data(medianPts)
                    .enter()
                    .append("circle")
                    .attr("class", "hm-median-hit")
                    .attr("cx", function (d) { return d.x; })
                    .attr("cy", function (d) { return d.y; })
                    .attr("r", 10)
                    .attr("fill", "transparent")
                    .each(function (d, i) {
                        var hitNode    = this;
                        var visibleDot = trendDotNodes[i];
                        var metricMeta = m;
                        hitNode.addEventListener("mouseover", function (event) {
                            d3.select(visibleDot).attr("r", 5);
                            tooltip.html(buildMedianTooltip(metricMeta, d, baseline))
                                   .classed("visible", true);
                            positionTooltip(event);
                        });
                        hitNode.addEventListener("mousemove", function (event) {
                            positionTooltip(event);
                        });
                        hitNode.addEventListener("mouseout", function () {
                            d3.select(visibleDot).attr("r", 3);
                            tooltip.classed("visible", false);
                        });
                    });

                if (animate) {
                    var totalLen = trendPath.node().getTotalLength();
                    var lineDelay = years.length * 55;
                    trendPath
                        .attr("stroke-dasharray", totalLen + " " + totalLen)
                        .attr("stroke-dashoffset", totalLen)
                        .transition()
                          .delay(lineDelay)
                          .duration(700)
                          .ease(d3.easeCubicOut)
                          .attr("stroke-dashoffset", 0);

                    trendDots
                        .attr("opacity", 0)
                        .transition()
                          .delay(function (d, i) { return lineDelay + i * 70; })
                          .duration(180)
                          .attr("opacity", 1);

                    trendLabel
                        .attr("opacity", 0)
                        .transition()
                          .delay(lineDelay + 700)
                          .duration(220)
                          .attr("opacity", 1);
                }
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

        function buildMedianTooltip(m, d, baseline) {
            var valueStr = fmtMedianValue(m, d.value);
            var deltaLine = "";
            if (baseline && baseline.year !== d.year) {
                var deltaStr = fmtMedianDelta(m, d.value - baseline.value);
                deltaLine =
                    "<span class='tt-count'>" +
                        "<span class='tt-count-pct'>" + deltaStr + "</span>" +
                        "<span class='tt-count-abs'>vs " + baseline.year + "</span>" +
                    "</span>";
            }
            return "<span class='tt-year'>"   + d.year + "</span>" +
                   "<span class='tt-metric'>Median " + m.label + "</span>" +
                   "<span class='tt-range'>"  + valueStr + "</span>" +
                   deltaLine;
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