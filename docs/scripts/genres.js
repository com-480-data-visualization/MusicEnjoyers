(function () {
    "use strict";

    var prefersReducedMotion =
        window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    var TRANS = prefersReducedMotion ? 0 : 600;

    // =====================================================
    // SHARED CHART DIMENSIONS (all 3 genre-section charts)
    // =====================================================
    const CHART_W = 580;
    const CHART_H = 360;

    // =====================================================
    // GENRE SHARE HELPERS
    // =====================================================
    const font_sz   = "9px";
    const axisColor = "#B3B3B3";
    const lineColor = "#444";
    const margin    = { top: 15, right: 20, bottom: 20, left: 35 };
    const innerWidth  = CHART_W - margin.left - margin.right;
    const innerHeight = CHART_H - margin.top  - margin.bottom;

    // =====================================================
    // GENRE COLOUR PALETTE
    // =====================================================
    const GENRE_COLORS = {
        "country"         : "#F59E0B",
        "christmas"       : "#34D399",
        "adult standards" : "#60A5FA",
        "rap"             : "#F87171",
        "rockabilly"      : "#A78BFA",
        "r&b"             : "#2DD4BF",
        "doo-wop"         : "#F472B6",
        "acoustic country": "#FB923C",
        "hip hop"         : "#9CA3AF",
        "melodic rap"     : "#A3E635",
        "other"           : "#6B7280"
    };
    function gc(g) { return GENRE_COLORS[g] || "#6B7280"; }

    // =====================================================
    // SHARED TOOLTIP
    // =====================================================
    const tooltip = d3.select("body")
        .append("div")
        .attr("class", "trends-tooltip");

    function showTip(event, html) {
        tooltip.html(html).classed("visible", true);
        moveTip(event);
    }
    function moveTip(event) {
        const n  = tooltip.node();
        const tw = n.offsetWidth;
        let   px = event.pageX + 14;
        const py = event.pageY - 16;
        if (px + tw > window.innerWidth - 20) px = event.pageX - tw - 14;
        tooltip.style("left", px + "px").style("top", py + "px");
    }
    function hideTip() { tooltip.classed("visible", false); }

    // =====================================================
    // SIDEBAR DESCRIPTIONS (one per tab)
    // =====================================================
    const sidebarDesc = {
        share: {
            h2: "Genres are shifting.",
            p:  "Country music dominates this decade, showing that pop music remains deeply rooted in country. <br/>" +
                "Rap peaked early and gradually faded, but its influence lived on — hip-hop, melodic rap, and R&B stepped in to fill the gap. <br/>" +
                "The steady presence of adult standards and classic Christmas tracks suggests that listeners are increasingly drawn to older, more timeless sounds."
        },
        radar: {
            h2: "Every genre has a voice.",
            p:  "Most genres stayed to their core sound over the decade. However, a few notable shifts stand out. <br/>" +
                "Hip-hop became darker and more introspective, melodic rap slowed down significantly, and R&B along with doo-wop drifted toward moodier, more stripped-back sounds.<br/>" +
                "Rockabilly, on the other hand, moved toward a more electronic, danceable style."
        },
        scatter: {
            h2: "Songs cluster by features.",
            p:  "To examine pop music trends without relying on predefined genres, we plotted each song as a point in feature space — where the axes represent audio features like danceability, valence, energy, and more.<br/>" +
                "By dragging the slider across years, you can watch the cloud shift, revealing how the overall sonic landscape of popular music has evolved over the decade."
        },
        cluster: {
            h2: "Sounds are converging.",
            p:  "Songs are clustered each year by Danceability, Energy, Tempo, Valence, and Acousticness (k=5). <br/>" +
                "Compactness (avg. distance to centroid) falls over time — songs within each cluster sound more alike. <br/>" +
                "Separation (silhouette score) also drops — clusters grow harder to tell apart. <br/>" +
                "Together, these trends show that pop music's sonic palette is converging: genres are blending into a shared sound. <br/>" +
                "(Click the chart to toggle between the two views.)"
        }
    };

    function updateSidebar(target) {
        const desc = sidebarDesc[target];
        if (!desc) return;
        const h2 = document.querySelector(".genre-sidebar h2");
        const p  = document.querySelector(".genre-sidebar p");
        if (h2) h2.innerHTML  = desc.h2;
        if (p)  p.innerHTML = desc.p;
    }

    // =====================================================
    // TAB SWITCHING
    // =====================================================
    const tabs   = document.querySelectorAll(".genre-tab");
    const panels = document.querySelectorAll(".genre-panel");

    function updateFeatPicker(target) {
        const picker = document.getElementById("scatter-feat-picker");
        if (picker) picker.classList.toggle("visible", target === "scatter");
    }

    tabs.forEach(tab => {
        tab.addEventListener("click", () => {
            tabs.forEach(t => t.classList.remove("active"));
            panels.forEach(p => p.classList.remove("active"));
            tab.classList.add("active");
            const target = tab.dataset.chart;
            document.getElementById(target + "-panel").classList.add("active");
            updateSidebar(target);
            updateFeatPicker(target);
        });
    });

    updateSidebar("share");
    updateFeatPicker("share");

    // =====================================================
    // DATA LOAD
    // =====================================================
    Promise.all([
        d3.json("data/genre_stats.json"),
        d3.json("data/cluster_stats.json"),
        d3.json("data/scatter_data.json"),
        d3.json("data/radar_data.json")
    ]).then(([genreJson, clusterData, scatterData, radarData]) => {
        drawGenreShareChart(genreJson);
        drawClusterChart(clusterData);
        drawScatterPlot(scatterData);
        drawRadarChart(radarData);
    }).catch(err => {
        console.warn("genres.js data load error:", err);
    });

    // =====================================================
    // COMMON SVG FACTORY
    // =====================================================
    function createSVG(selector) {
        const svg = d3.select(selector)
            .append("svg")
            .attr("viewBox", `0 0 ${CHART_W} ${CHART_H}`)
            .attr("preserveAspectRatio", "xMidYMid meet");
        const g = svg.append("g")
            .attr("transform", `translate(${margin.left},${margin.top})`);
        return { svg, g };
    }

    function styleAxis(g) {
        g.selectAll("text")
            .style("fill", axisColor)
            .style("font-size", font_sz);
        g.selectAll(".domain, .tick line")
            .style("stroke", lineColor);
    }

    // =====================================================
    // CHART 1 : STACKED AREA (Genre Share)
    // =====================================================
    function drawGenreShareChart(jsonData) {
        const data = jsonData.data;
        const keys = jsonData.keys;

        const { svg, g } = createSVG("#genreChart");

        const color = d3.scaleOrdinal()
            .domain(keys)
            .range([
                "#1DB954", "#1ED760", "#3D9E4F", "#61D56F", "#55B37A",
                "#4D8A5E", "#2C7D4F", "#7DDC92", "#8FD7A9", "#76C384"
            ]);

        const stack = d3.stack().keys(keys).offset(d3.stackOffsetExpand);
        const layers = stack(data);

        const x = d3.scaleLinear().domain(d3.extent(data, d => d.year)).range([0, innerWidth]);
        const y = d3.scaleLinear().domain([0, 1]).range([innerHeight, 0]);

        const area = d3.area()
            .x(d => x(d.data.year))
            .y0(d => y(d[0]))
            .y1(d => y(d[1]))
            .curve(d3.curveCardinal);
        const initialArea = d3.area()
            .x(d => x(d.data.year))
            .y0(innerHeight)
            .y1(innerHeight)
            .curve(d3.curveCardinal);

        const paths = g.selectAll(".layer")
            .data([...layers])
            .enter()
            .append("path")
            .attr("fill", d => color(d.key))
            .attr("opacity", 0.95)
            .attr("d", initialArea);

        function getPeakIndex(layer) {
            let maxIdx = 2, maxVal = -Infinity;
            for (let i = 2; i < layer.length - 3; i++) {
                const thickness = layer[i][1] - layer[i][0];
                if (thickness > maxVal) { maxVal = thickness; maxIdx = i; }
            }
            return maxIdx;
        }

        const labels = g.selectAll(".area-label").data(layers).enter().append("text")
            .attr("class", "area-label").attr("text-anchor", "middle")
            .style("fill", d => d3.color(color(d.key)).brighter(-2))
            .attr("clip-path", (d, i) => `url(#clip-${i})`)
            .style("font-size", d => {
                const i = getPeakIndex(d);
                const thickness = d[i][1] - d[i][0];
                return (thickness * 230).toString() + "px";
            })
            .style("font-weight", "700").style("opacity", 0)
            .text(d => d.key)
            .attr("x", d => x(d[getPeakIndex(d)].data.year))
            .attr("y", d => { const p = d[getPeakIndex(d)]; return y(p[0]) - 4; });

        const labels_fade = g.selectAll(".labels_fade").data(layers).enter().append("text")
            .attr("class", "labels_fade").attr("text-anchor", "middle")
            .style("fill", d => d3.color(color(d.key)).brighter(-2))
            .style("font-size", d => {
                const i = getPeakIndex(d);
                const thickness = d[i][1] - d[i][0];
                return (thickness * 230).toString() + "px";
            })
            .style("font-weight", "700").style("opacity", 0)
            .text(d => d.key)
            .attr("x", d => x(d[getPeakIndex(d)].data.year))
            .attr("y", d => { const p = d[getPeakIndex(d)]; return y(p[0]) - 4; });

        const clips = g.append("defs").selectAll("clipPath")
            .data(layers).enter().append("clipPath").attr("id", (d, i) => `clip-${i}`);
        clips.append("path").attr("d", d => area(d));

        const xAxis = g.append("g")
            .attr("transform", `translate(0,${innerHeight})`)
            .call(d3.axisBottom(x).ticks(10).tickFormat(d3.format("d")).tickSize(0));

        xAxis.selectAll(".tick text").each(function(d, i) {
            const text = d3.select(this);
            if (i === 0) text.attr("text-anchor", "start");
            else if (i === xAxis.selectAll(".tick").size() - 1) text.attr("text-anchor", "end");
            else text.attr("text-anchor", "middle");
        });

        const yAxis = g.append("g")
            .call(d3.axisLeft(y).tickValues([0, 0.5, 1]).tickFormat(d => `${d * 100}%`).tickSize(0));

        g.selectAll(".domain").remove();
        styleAxis(xAxis);
        styleAxis(yAxis);

        const observer = new IntersectionObserver(entries => {
            entries.forEach(entry => {
                if (!entry.isIntersecting) return;
                paths.transition().delay((d, i) => i * 180).duration(1200).ease(d3.easeCubicOut).attr("d", area);
                clips.select("path").transition().delay((d, i) => i * 180).duration(1200).ease(d3.easeCubicOut).attr("d", d => area(d));
                labels.attr("opacity", 0).transition().delay((d, i) => i * 180 + 700).duration(600).style("opacity", 1);
                labels_fade.attr("opacity", 0).transition().delay((d, i) => i * 180 + 700).duration(600).style("opacity", 0.3);
                observer.unobserve(entry.target);
            });
        }, { threshold: 0.2 });
        observer.observe(document.getElementById("genreChart"));
    }

    // =====================================================
    // CHART 2 : SCATTER PLOT (Feature Space)
    // =====================================================
    function drawScatterPlot(allData) {
        const FEATURES = {
            danceability:  "Danceability",
            energy:        "Energy",
            acousticness:  "Acousticness",
            valence:       "Valence",
            speechiness:   "Speechiness",
            tempo_norm:    "Tempo"
        };
        const FEAT_KEYS = Object.keys(FEATURES);

        // Pre-compute global extent per feature across all years (stable axes during animation)
        const globalExtent = {};
        FEAT_KEYS.forEach(k => {
            const vals = [];
            Object.keys(allData).forEach(yr => {
                (allData[yr] || []).forEach(d => {
                    const v = parseFloat(d[k]);
                    if (!isNaN(v)) vals.push(v);
                });
            });
            if (vals.length < 2) { globalExtent[k] = [0, 1]; return; }
            const lo = d3.min(vals), hi = d3.max(vals);
            const pad = (hi - lo) * 0.05 || 0.05;
            globalExtent[k] = [lo - pad, hi + pad];
        });

        const W = 490, H = 350;
        const sm = { top: 18, right: 24, bottom: 44, left: 50 };
        const iW = W - sm.left - sm.right;
        const iH = H - sm.top  - sm.bottom;

        const years = Object.keys(allData).map(Number).sort((a, b) => a - b);
        let currentYear = years[0];
        let playing = false, playTimer = null;
        let xKey = "danceability", yKey = "energy";

        // ── Build SVG ─────────────────────────────────────
        const svg = d3.select("#scatter-plot")
            .append("svg")
            .attr("viewBox", `0 0 ${W} ${H}`)
            .attr("preserveAspectRatio", "xMinYMid meet");
        const g = svg.append("g").attr("transform", `translate(${sm.left},${sm.top})`);

        const xSc = d3.scaleLinear().range([0, iW]);
        const ySc = d3.scaleLinear().range([iH, 0]);

        // Grid + tick containers (updated when axes change)
        const xGridG = g.append("g");
        const yGridG = g.append("g");
        const xAxG   = g.append("g").attr("transform", `translate(0,${iH})`);
        const yAxG   = g.append("g");

        const xAxisLbl = g.append("text").attr("x", iW / 2).attr("y", iH + 36)
            .attr("text-anchor", "middle").style("fill", "#B3B3B3").style("font-size", "12px");
        const yAxisLbl = g.append("text").attr("transform", "rotate(-90)")
            .attr("x", -iH / 2).attr("y", -38)
            .attr("text-anchor", "middle").style("fill", "#B3B3B3").style("font-size", "12px");

        const yearLabel = g.append("text")
            .attr("x", iW - 4).attr("y", iH - 4).attr("text-anchor", "end")
            .style("fill", "rgba(255,255,255,0.06)")
            .style("font-size", "56px").style("font-weight", "900")
            .text(currentYear);

        const dotsG = g.append("g");

        function updateAxes() {
            xSc.domain(globalExtent[xKey]);
            ySc.domain(globalExtent[yKey]);

            xGridG.call(d3.axisLeft(ySc).ticks(5).tickSize(-iW).tickFormat(""))
                .call(ax => { ax.select(".domain").remove(); ax.selectAll(".tick line").style("stroke", "#222"); });
            yGridG.attr("transform", `translate(0,${iH})`)
                .call(d3.axisBottom(xSc).ticks(5).tickSize(-iH).tickFormat(""))
                .call(ax => { ax.select(".domain").remove(); ax.selectAll(".tick line").style("stroke", "#222"); });

            xAxG.call(d3.axisBottom(xSc).ticks(5).tickFormat(d3.format(".2f")).tickSize(0));
            yAxG.call(d3.axisLeft(ySc).ticks(5).tickFormat(d3.format(".2f")).tickSize(0));
            [xAxG, yAxG].forEach(ax => {
                ax.select(".domain").remove();
                ax.selectAll("text").style("fill", "#B3B3B3").style("font-size", "11px");
            });

            xAxisLbl.text(FEATURES[xKey]);
            yAxisLbl.text(FEATURES[yKey]);
        }

        // ── Render dots ───────────────────────────────────
        function render(animate) {
            const raw = allData[String(currentYear)] || [];
            const points = raw.filter(d =>
                !isNaN(parseFloat(d[xKey])) && !isNaN(parseFloat(d[yKey]))
            );
            const dur = (animate && !prefersReducedMotion) ? TRANS * 0.75 : 0;
            yearLabel.text(currentYear);
            dotsG.selectAll(".sc-dot").interrupt().remove();
            dotsG.selectAll(".sc-dot")
                .data(points).enter()
                .append("circle").attr("class", "sc-dot")
                .attr("r", 4.5)
                .attr("cx", d => xSc(parseFloat(d[xKey])))
                .attr("cy", d => ySc(parseFloat(d[yKey])))
                .attr("fill", d => gc(d.genre))
                .attr("stroke", "#0d0d0d").attr("stroke-width", 0.8)
                .attr("opacity", 0)
                .on("mouseover", function(event, d) {
                    d3.select(this).attr("r", 7).attr("opacity", 1);
                    showTip(event,
                        "<strong style='color:" + gc(d.genre) + "'>" + d.title + "</strong><br>" +
                        "<span style='color:#B3B3B3'>" + d.artist + "</span><br>" +
                        "Genre: " + d.genre + "<br>" +
                        FEATURES[xKey] + ": <strong>" + parseFloat(d[xKey]).toFixed(2) + "</strong><br>" +
                        FEATURES[yKey] + ": <strong>" + parseFloat(d[yKey]).toFixed(2) + "</strong>"
                    );
                })
                .on("mousemove", function(event) { moveTip(event); })
                .on("mouseleave", function() {
                    d3.select(this).attr("r", 4.5).attr("opacity", 0.72);
                    hideTip();
                })
                .transition().duration(dur).ease(d3.easeQuadInOut)
                .attr("opacity", 0.72);
        }

        // ── Feature picker (single row, push mechanic) ────
        // Clicking an inactive button → it becomes X, old X becomes Y
        // Clicking the current X button → swap X and Y
        const picker = document.getElementById("scatter-feat-picker");
        const btnMap = {};

        function refreshButtons() {
            Object.keys(btnMap).forEach(k => {
                const btn = btnMap[k];
                btn.classList.remove("active-x", "active-y");
                if (k === xKey) btn.classList.add("active-x");
                else if (k === yKey) btn.classList.add("active-y");
            });
        }

        if (picker) {
            const axisLegend = document.createElement("div");
            axisLegend.className = "scatter-axis-legend";
            axisLegend.innerHTML =
                "<span><span class='scatter-axis-swatch' style='background:#1DB954'></span>X axis</span>" +
                "<span><span class='scatter-axis-swatch' style='background:#60A5FA'></span>Y axis</span>";
            picker.appendChild(axisLegend);

            const row = document.createElement("div");
            row.className = "scatter-feat-row";
            FEAT_KEYS.forEach(k => {
                const btn = document.createElement("button");
                btn.className = "scatter-feat-btn";
                btn.textContent = FEATURES[k];
                btnMap[k] = btn;
                btn.addEventListener("click", function() {
                    if (k === xKey) {
                        [xKey, yKey] = [yKey, xKey]; // swap
                    } else {
                        yKey = xKey; // old X → Y
                        xKey = k;
                    }
                    refreshButtons();
                    updateAxes();
                    render(false);
                });
                row.appendChild(btn);
            });
            picker.appendChild(row);
        }

        refreshButtons();
        updateAxes();

        // ── Year controls ─────────────────────────────────
        const slider   = document.getElementById("year-slider");
        const yearDisp = document.getElementById("scatter-year-display");
        const playBtn  = document.getElementById("play-btn");

        function setYear(yr, animate) {
            currentYear = yr;
            if (slider)    slider.value        = yr;
            if (yearDisp)  yearDisp.textContent = yr;
            render(animate);
        }

        if (slider) slider.addEventListener("input", function() { setYear(+this.value, false); });

        if (playBtn) {
            playBtn.addEventListener("click", function() {
                if (playing) {
                    clearInterval(playTimer); playing = false;
                    playBtn.innerHTML = "&#9654; Play";
                } else {
                    if (currentYear >= years[years.length - 1]) setYear(years[0], false);
                    playing = true;
                    playBtn.innerHTML = "&#9646;&#9646; Pause";
                    playTimer = setInterval(function() {
                        const ni = years.indexOf(currentYear) + 1;
                        if (ni >= years.length) {
                            clearInterval(playTimer); playing = false;
                            playBtn.innerHTML = "&#9654; Play";
                        } else { setYear(years[ni], true); }
                    }, 1200);
                }
            });
        }

        setYear(currentYear, false);

        // ── Legend ────────────────────────────────────────
        const seen = {};
        Object.values(allData).forEach(pts => pts.forEach(d => { seen[d.genre] = true; }));
        const legEl = document.getElementById("sc-legend");
        if (legEl) {
            Object.keys(GENRE_COLORS).forEach(k => {
                if (!seen[k]) return;
                const chip = document.createElement("span");
                chip.className = "trends-chip";
                chip.innerHTML = "<span class='trends-chip-swatch' style='background:" + gc(k) + "'></span>" + k;
                legEl.appendChild(chip);
            });
        }
    }

    // =====================================================
    // CHART 3 : RADAR CHART (Fingerprint)
    // =====================================================
    function drawRadarChart(radarData) {
        const W = CHART_W, H = CHART_H;
        const cx = W / 2, cy = H / 2 - 8;
        const R  = 118;

        const AXES   = ["energy", "danceability", "acousticness", "valence", "speechiness", "tempo_norm"];
        const LABELS = ["Energy", "Danceability", "Acousticness", "Valence", "Speechiness", "Tempo"];
        const N = AXES.length;

        const genres = radarData.genres;
        const sel    = document.getElementById("genre-select");

        genres.forEach(genre => {
            const opt = document.createElement("option");
            opt.value = genre;
            opt.textContent = genre.charAt(0).toUpperCase() + genre.slice(1);
            sel.appendChild(opt);
        });

        const svg = d3.select("#radar-chart")
            .append("svg")
            .attr("viewBox", `0 0 ${W} ${H}`)
            .attr("preserveAspectRatio", "xMinYMid meet");

        const g = svg.append("g").attr("transform", `translate(${cx},${cy})`);

        function axisAngle(i) { return (2 * Math.PI * i / N) - Math.PI / 2; }
        function polar(i, v) {
            const a = axisAngle(i);
            return [Math.cos(a) * v * R, Math.sin(a) * v * R];
        }

        [0.25, 0.5, 0.75, 1].forEach(lv => {
            const pts = AXES.map((_, i) => polar(i, lv).join(",")).join(" ");
            g.append("polygon").attr("points", pts)
                .attr("fill", "none").attr("stroke", "#2a2a2a").attr("stroke-width", 1);
        });

        [0.25, 0.5, 0.75, 1].forEach(lv => {
            g.append("text").attr("x", 3).attr("y", -lv * R - 3)
                .style("fill", "#3a3a3a").style("font-size", "9px")
                .text(lv.toFixed(2));
        });

        AXES.forEach((_, i) => {
            const pt = polar(i, 1);
            g.append("line").attr("x1", 0).attr("y1", 0).attr("x2", pt[0]).attr("y2", pt[1])
                .attr("stroke", "#333").attr("stroke-width", 1);
            const lp     = polar(i, 1.28);
            const anchor = lp[0] > 6 ? "start" : lp[0] < -6 ? "end" : "middle";
            g.append("text").attr("x", lp[0]).attr("y", lp[1])
                .attr("text-anchor", anchor).attr("dy", "0.35em")
                .style("fill", "#B3B3B3").style("font-size", "11px")
                .text(LABELS[i]);
        });

        function genrePts(gKey, yr) {
            const rec = ((radarData.data[gKey] || {})[String(yr)]) || {};
            return AXES.map((ax, i) => {
                const v = rec[ax] != null ? rec[ax] : 0;
                return polar(i, Math.min(1, Math.max(0, v)));
            });
        }
        function ptsStr(pts) { return pts.map(p => p.join(",")).join(" "); }

        const poly16 = g.append("polygon")
            .attr("fill", "rgba(255,255,255,0.08)")
            .attr("stroke", "#aaaaaa").attr("stroke-width", 1.5)
            .attr("stroke-dasharray", "5,3");
        const poly25 = g.append("polygon")
            .attr("fill", "rgba(29,185,84,0.15)")
            .attr("stroke", "#1DB954").attr("stroke-width", 2);

        const dots16 = g.selectAll(".rd16").data(AXES).enter()
            .append("circle").attr("class", "rd16").attr("r", 3).attr("fill", "#aaaaaa");
        const dots25 = g.selectAll(".rd25").data(AXES).enter()
            .append("circle").attr("class", "rd25").attr("r", 3).attr("fill", "#1DB954");

        function update(genre, animate) {
            const p16 = genrePts(genre, 2016);
            const p25 = genrePts(genre, 2025);
            const dur = (animate && !prefersReducedMotion) ? TRANS : 0;
            if (dur > 0) {
                poly16.transition().duration(dur).ease(d3.easeQuadInOut).attr("points", ptsStr(p16));
                poly25.transition().duration(dur).ease(d3.easeQuadInOut).attr("points", ptsStr(p25));
                dots16.data(p16).transition().duration(dur).ease(d3.easeQuadInOut)
                    .attr("cx", d => d[0]).attr("cy", d => d[1]);
                dots25.data(p25).transition().duration(dur).ease(d3.easeQuadInOut)
                    .attr("cx", d => d[0]).attr("cy", d => d[1]);
            } else {
                poly16.attr("points", ptsStr(p16));
                poly25.attr("points", ptsStr(p25));
                dots16.data(p16).attr("cx", d => d[0]).attr("cy", d => d[1]);
                dots25.data(p25).attr("cx", d => d[0]).attr("cy", d => d[1]);
            }
        }

        const legG = svg.append("g").attr("transform", `translate(${cx - 60},${H - 28})`);
        [
            { label: "2016", stroke: "#aaaaaa", fill: "rgba(255,255,255,0.08)", dash: "5,3" },
            { label: "2025", stroke: "#1DB954", fill: "rgba(29,185,84,0.15)",   dash: null  }
        ].forEach((d, i) => {
            const lg = legG.append("g").attr("transform", `translate(${i * 90},0)`);
            lg.append("rect").attr("width", 14).attr("height", 14).attr("rx", 2)
                .attr("fill", d.fill).attr("stroke", d.stroke).attr("stroke-width", 1.5)
                .attr("stroke-dasharray", d.dash || "");
            lg.append("text").attr("x", 18).attr("y", 11)
                .style("fill", "#B3B3B3").style("font-size", "12px").text(d.label);
        });

        if (sel) {
            sel.addEventListener("change", function() { update(this.value, true); });
        }

        update(genres[0], false);
    }
    
    
    // =====================================================
    // CHART 4 : CLUSTER METRICS (element not in DOM, no-op)
    // =====================================================
    function drawClusterChart(clusterData) {
        const { svg, g } = createSVG("#clusterChart");
        let activeMetric = "compactness";
        const axisPad = 28;
        const years = clusterData.map(d => d.year);
        const x = d3.scaleLinear().domain(d3.extent(years)).range([axisPad, innerWidth - axisPad]);
        const yCompact  = d3.scaleLinear().domain(d3.extent(clusterData, d => d.avg_distance)).nice().range([innerHeight, 0]);
        const ySeparate = d3.scaleLinear().domain(d3.extent(clusterData, d => d.silhouette)).nice().range([innerHeight, 0]);
        const lineCompact  = d3.line().x(d => x(d.year)).y(d => yCompact(d.avg_distance)).curve(d3.curveMonotoneX);
        const lineSeparate = d3.line().x(d => x(d.year)).y(d => ySeparate(d.silhouette)).curve(d3.curveMonotoneX);
        const compactLine  = g.append("path").datum(clusterData).attr("fill","none").attr("stroke","#1DB954").attr("stroke-width",2).attr("d",lineCompact);
        const separateLine = g.append("path").datum(clusterData).attr("fill","none").attr("stroke","#1DB954").attr("stroke-width",2).attr("d",lineSeparate);
        const xAxis     = g.append("g").attr("transform",`translate(0,${innerHeight})`).call(d3.axisBottom(x).tickValues(years).tickFormat(d3.format("d")).tickSize(0));
        const yAxisLeft = g.append("g");
        const yAxisRight= g.append("g").attr("transform",`translate(${innerWidth},0)`);
        styleAxis(xAxis);
        const title_compactness = g.append("text").attr("x",30).attr("y",0).attr("text-anchor","begin").style("font-size","15px").style("font-weight","700").style("fill","rgba(255,255,255,0.08)").style("pointer-events","none").text("COMPACTNESS");
        const title_separation  = g.append("text").attr("x",innerWidth-50).attr("y",0).attr("text-anchor","end").style("font-size","15px").style("font-weight","700").style("fill","rgba(255,255,255,0.08)").style("pointer-events","none").text("SEPARATION");
        function updateChart() {
            if (activeMetric === "compactness") {
                title_compactness.style("fill","#1DB954"); title_separation.style("fill","rgba(255,255,255,0.08)");
                compactLine.transition().duration(250).attr("opacity",1).attr("stroke-width",2);
                separateLine.transition().duration(250).attr("opacity",0.12).attr("stroke-width",1.5);
            } else {
                title_compactness.style("fill","rgba(255,255,255,0.08)"); title_separation.style("fill","#1DB954");
                separateLine.transition().duration(250).attr("opacity",1).attr("stroke-width",2);
                compactLine.transition().duration(250).attr("opacity",0.12).attr("stroke-width",1.5);
            }
            yAxisLeft.call(d3.axisLeft(yCompact).ticks(5).tickSize(0));
            yAxisRight.call(d3.axisRight(ySeparate).ticks(5).tickSize(0));
            styleAxis(yAxisLeft); styleAxis(yAxisRight);
            yAxisLeft.selectAll("text").style("fill","#9a9a9a").style("font-size",font_sz);
            yAxisRight.selectAll("text").style("fill","#9a9a9a").style("font-size",font_sz);
            xAxis.selectAll("text").style("fill","#9a9a9a").style("font-size",font_sz);
        }
        svg.style("cursor","pointer").on("click", () => {
            activeMetric = activeMetric === "compactness" ? "separation" : "compactness";
            updateChart();
        });
        updateChart();
    }
})();