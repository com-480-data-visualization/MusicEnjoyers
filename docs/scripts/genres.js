(function () {

    // =====================================================
    // GLOBAL SETTINGS
    // =====================================================

    const tabs = document.querySelectorAll(".genre-tab");
    const panels = document.querySelectorAll(".genre-panel");

    const width = 420;
    const height = 200;
    const font_sz = "6px";
    const margin = {
        top: 30,
        right: 55,
        bottom: 30,
        left: 55
    };

    const innerWidth = width - margin.left - margin.right;
    const innerHeight = height - margin.top - margin.bottom;

    const axisColor = "#B3B3B3";
    const lineColor = "#444";

    // =====================================================
    // TAB SWITCHING
    // =====================================================

    tabs.forEach(tab => {
        tab.addEventListener("click", () => {

            tabs.forEach(t => t.classList.remove("active"));
            panels.forEach(p => p.classList.remove("active"));

            tab.classList.add("active");

            const target = tab.dataset.chart;
            document.getElementById(target + "-panel").classList.add("active");
        });
    });

    // =====================================================
    // DATA LOAD
    // =====================================================

    Promise.all([
        d3.json("data/genre_stats.json"),
        d3.json("data/cluster_stats.json")
    ]).then(([genreJson, clusterData]) => {
        drawGenreShareChart(genreJson);
        drawTrackCountChart(genreJson);
        drawClusterChart(clusterData);
    });

    // =====================================================
    // COMMON SVG
    // =====================================================

    function createSVG(selector) {

        const svg = d3.select(selector)
            .append("svg")
            .attr("viewBox", `0 0 ${width} ${height}`)
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
    // CHART 1 : STACKED AREA
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

        const area = d3.area().x(d => x(d.data.year)).y0(d => y(d[0])).y1(d => y(d[1])).curve(d3.curveCardinal);
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

        // labels inside areas
        function getPeakIndex(layer) {
            let maxIdx = 2;
            let maxVal = -Infinity;
            for (let i = 2; i < layer.length - 3; i++) {
                const thickness = layer[i][1] - layer[i][0];
                if (thickness > maxVal) {
                    maxVal = thickness;
                    maxIdx = i;
                }
            }
            return maxIdx;
        }

        const labels = g.selectAll(".area-label").data(layers).enter().append("text")
            .attr("class", "area-label").attr("text-anchor", "middle")
            .style("fill", d => { return d3.color(color(d.key)).brighter(-2); })
            .attr("clip-path", (d, i) => `url(#clip-${i})`)
            .style("font-size", d => {
                const i = getPeakIndex(d);
                const thickness = d[i][1] - d[i][0];
                return (thickness * 100).toString() + "px";
            })
            .style("font-weight", "700").style("opacity", 0)
            .text(d => d.key)
            .attr("x", d => {
                const i = getPeakIndex(d);
                return x(d[i].data.year);
            })
            .attr("y", d => {
                const i = getPeakIndex(d);
                const p = d[i];
                return y(p[0]) - 4;
            });

        const labels_fade = g.selectAll(".labels_fade").data(layers).enter().append("text")
            .attr("class", "labels_fade").attr("text-anchor", "middle")
            .style("fill", d => { return d3.color(color(d.key)).brighter(-2); })
            .style("font-size", d => {
                const i = getPeakIndex(d);
                const thickness = d[i][1] - d[i][0];
                return (thickness * 100).toString() + "px";
            })
            .style("font-weight", "700")
            .style("opacity", 0)
            .text(d => d.key)
            .attr("x", d => {
                const i = getPeakIndex(d);
                return x(d[i].data.year);
            })
            .attr("y", d => {
                const i = getPeakIndex(d);
                const p = d[i];
                return y(p[0]) - 4;
            });

        const clips = g.append("defs").selectAll("clipPath")
            .data(layers).enter().append("clipPath").attr("id", (d, i) => `clip-${i}`);

        clips.append("path").attr("d", d => area(d));
        
        // axes
        const xAxis = g.append("g")
            .attr("transform", `translate(0,${innerHeight})`)
            .call(
                d3.axisBottom(x)
                    .ticks(10)
                    .tickFormat(d3.format("d"))
                    .tickSize(0)
            );

        xAxis.selectAll(".tick text")
            .each(function(d, i) {
                const text = d3.select(this);

                if (i === 0) {
                    text.attr("text-anchor", "start");
                }
                else if (i === xAxis.selectAll(".tick").size() - 1) {
                    text.attr("text-anchor", "end");
                }
                else {
                    text.attr("text-anchor", "middle");
                }
            });

        const yAxis = g.append("g")
            .call(
                d3.axisLeft(y)
                    .tickValues([0, 0.5, 1])
                    .tickFormat(d => `${d * 100}%`)
                    .tickSize(0)
            );
            
        g.selectAll(".domain").remove();
        styleAxis(xAxis);
        styleAxis(yAxis);

        // animation
        const observer = new IntersectionObserver(entries => {
            entries.forEach(entry => {
                if (!entry.isIntersecting) return;

                paths.transition()
                    .delay((d, i) => i * 180)
                    .duration(1200)
                    .ease(d3.easeCubicOut)
                    .attr("d", area);

                clips.select("path")
                    .transition()
                    .delay((d, i) => i * 180)
                    .duration(1200)
                    .ease(d3.easeCubicOut)
                    .attr("d", d => area(d));
                labels
                    .attr("opacity", 0)
                    .transition()
                    .delay((d, i) => i * 180 + 700)
                    .duration(600)
                    .style("opacity", 1);
                labels_fade
                    .attr("opacity", 0)
                    .transition()
                    .delay((d, i) => i * 180 + 700)
                    .duration(600)
                    .style("opacity", 0.3);
                

                observer.unobserve(entry.target);
            });
        }, { threshold: 0.2 });
        observer.observe(document.getElementById("genreChart"));
    }

    // =====================================================
    // CHART 2 : LINE CHART
    // =====================================================

    function drawTrackCountChart(jsonData) {

        const data = jsonData.data;
        const keys = jsonData.keys;

        const { g } = createSVG("#genreLineChart");

        const color = d3.scaleOrdinal()
            .domain(keys)
            .range(d3.schemeTableau10);

        const x = d3.scaleLinear()
            .domain(d3.extent(data, d => d.year))
            .range([0, innerWidth]);

        const y = d3.scaleLinear()
            .domain([
                0,
                d3.max(data, d => d3.max(keys, k => d[k]))
            ])
            .nice()
            .range([innerHeight, 0]);

        keys.forEach(key => {

            const line = d3.line()
                .x(d => x(d.year))
                .y(d => y(d[key]))
                .curve(d3.curveMonotoneX);

            g.append("path")
                .datum(data)
                .attr("fill", "none")
                .attr("stroke", color(key))
                .attr("stroke-width", 2)
                .attr("d", line);
        });

        const xAxis = g.append("g")
            .attr("transform", `translate(0,${innerHeight})`)
            .call(
                d3.axisBottom(x)
                    .ticks(10)
                    .tickFormat(d3.format("d"))
            );

        const yAxis = g.append("g")
            .call(d3.axisLeft(y).ticks(5));

        styleAxis(xAxis);
        styleAxis(yAxis);
    }

    // =====================================================
    // CHART 3 : CLUSTER METRICS
    // click chart to switch
    // more padding from y-axes
    // thinner lines / smaller dots / neutral axes
    // =====================================================

    function drawClusterChart(clusterData) {

        const { svg, g } = createSVG("#clusterChart");

        // -------------------------------------------------
        // STATE
        // -------------------------------------------------

        let activeMetric = "compactness";
        const axisPad = 28;

        // -------------------------------------------------
        // DATA / SCALE
        // -------------------------------------------------

        const years = clusterData.map(d => d.year);
        const x = d3.scaleLinear()
            .domain(d3.extent(years))
            .range([axisPad, innerWidth - axisPad]);

        const yCompact = d3.scaleLinear()
            .domain(d3.extent(clusterData, d => d.avg_distance))
            .nice()
            .range([innerHeight, 0]);

        const ySeparate = d3.scaleLinear()
            .domain(d3.extent(clusterData, d => d.silhouette))
            .nice()
            .range([innerHeight, 0]);

        // -------------------------------------------------
        // STRAIGHT LINE
        // -------------------------------------------------

        const lineCompact = d3.line()
            .x(d => x(d.year))
            .y(d => yCompact(d.avg_distance));

        const lineSeparate = d3.line()
            .x(d => x(d.year))
            .y(d => ySeparate(d.silhouette));

        // -------------------------------------------------
        // DRAW LINES
        // -------------------------------------------------

        const compactLine = g.append("path")
            .datum(clusterData)
            .attr("fill", "none")
            .attr("stroke", "#1DB954")
            .attr("stroke-width", 2)
            .attr("d", lineCompact);

        const separateLine = g.append("path")
            .datum(clusterData)
            .attr("fill", "none")
            .attr("stroke", "#1DB954")
            .attr("stroke-width", 2)
            .attr("d", lineSeparate);

        // -------------------------------------------------
        // AXES
        // -------------------------------------------------

        const xAxis = g.append("g")
            .attr("transform", `translate(0,${innerHeight})`)
            .call(
                d3.axisBottom(x)
                    .tickValues(years)
                    .tickFormat(d3.format("d"))
                    .tickSize(0)
            );

        const yAxisLeft = g.append("g");

        const yAxisRight = g.append("g")
            .attr("transform", `translate(${innerWidth},0)`);

        styleAxis(xAxis);

        // -------------------------------------------------
        // BIG TITLE
        // -------------------------------------------------

        const title_compactness = g.append("text")
            .attr("x", 30)
            .attr("y", 0)
            .attr("text-anchor", "begin")
            .style("font-size", "15px")
            .style("font-weight", "700")
            .style("fill", "rgba(255,255,255,0.08)")
            .style("pointer-events", "none")
            .text("COMPACTNESS");
        
        const title_separation = g.append("text")
            .attr("x", innerWidth - 50)
            .attr("y", 0)
            .attr("text-anchor", "end")
            .style("font-size", "15px")
            .style("font-weight", "700")
            .style("fill", "rgba(255,255,255,0.08)")
            .style("pointer-events", "none")
            .text("SEPARATION");

        // -------------------------------------------------
        // UPDATE
        // -------------------------------------------------

        function updateChart() {

            if (activeMetric === "compactness") {

                title_compactness.style("fill", "#1DB954");
                title_separation.style("fill", "rgba(255,255,255,0.08)");

                compactLine.transition().duration(250)
                    .attr("opacity", 1)
                    .attr("stroke-width", 2);

                separateLine.transition().duration(250)
                    .attr("opacity", 0.12)
                    .attr("stroke-width", 1.5);

            } else {

                title_compactness.style("fill", "rgba(255,255,255,0.08)");
                title_separation.style("fill", "#1DB954");

                separateLine.transition().duration(250)
                    .attr("opacity", 1)
                    .attr("stroke-width", 2);

                compactLine.transition().duration(250)
                    .attr("opacity", 0.12)
                    .attr("stroke-width", 1.5);

            }

            yAxisLeft.call(
                d3.axisLeft(yCompact)
                    .ticks(5)
                    .tickSize(0)
            );

            yAxisRight.call(
                d3.axisRight(ySeparate)
                    .ticks(5)
                    .tickSize(0)
            );

            styleAxis(yAxisLeft);
            styleAxis(yAxisRight);

            yAxisLeft.selectAll("text")
                .style("fill", "#9a9a9a")
                .style("font-size", font_sz);

            yAxisRight.selectAll("text")
                .style("fill", "#9a9a9a")
                .style("font-size", font_sz);

            xAxis.selectAll("text")
                .style("fill", "#9a9a9a")
                .style("font-size", font_sz);
        }

        // -------------------------------------------------
        // CLICK SWITCH
        // -------------------------------------------------

        svg.style("cursor", "pointer");

        svg.on("click", () => {

            activeMetric =
                activeMetric === "compactness"
                    ? "separation"
                    : "compactness";

            updateChart();
        });

        updateChart();
    }

})();