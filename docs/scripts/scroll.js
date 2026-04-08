// Scroll-triggered animations using D3.js
// Handles reveal animations and active nav link tracking

(function() {

    // --- Reveal animations ---
    // Initialize all .reveal elements as hidden
    d3.selectAll('.reveal')
        .style('opacity', 0)
        .style('transform', 'translateY(60px)');

    const revealObserver = new IntersectionObserver(function(entries) {
        entries.forEach(function(entry) {
            const el = d3.select(entry.target);
            if (entry.isIntersecting) {
                el.transition()
                    .duration(800)
                    .ease(d3.easeCubicOut)
                    .style('opacity', 1)
                    .style('transform', 'translateY(0)');
            } else {
                el.transition()
                    .duration(400)
                    .style('opacity', 0)
                    .style('transform', 'translateY(60px)');
            }
        });
    }, { threshold: 0.15 });

    d3.selectAll('.reveal').each(function() {
        revealObserver.observe(this);
    });

    // --- Active nav link tracking ---
    const sections = d3.selectAll('section.section');
    const navLinks = d3.selectAll('.nav-link');

    const navObserver = new IntersectionObserver(function(entries) {
        entries.forEach(function(entry) {
            if (entry.isIntersecting) {
                const sectionId = entry.target.id;
                // Map section ids to nav href anchors
                var anchor = '#' + sectionId;
                // dashboard-section maps to #dashboard nav link
                if (sectionId === 'dashboard-section') {
                    anchor = '#dashboard';
                }

                navLinks.classed('active', false);
                d3.select('a.nav-link[href="' + anchor + '"]').classed('active', true);
            }
        });
    }, { threshold: 0.4 });

    sections.each(function() {
        navObserver.observe(this);
    });

    // --- Auto-hide header on scroll ---
    var lastScrollTop = 0;
    var header = d3.select('.top-nav');

    window.addEventListener('scroll', function() {
        var scrollTop = window.pageYOffset || document.documentElement.scrollTop;

        if (scrollTop > lastScrollTop && scrollTop > 100) {
            header.classed('hidden', true);
        } else {
            header.classed('hidden', false);
        }

        lastScrollTop = scrollTop <= 0 ? 0 : scrollTop;
    });

})();
