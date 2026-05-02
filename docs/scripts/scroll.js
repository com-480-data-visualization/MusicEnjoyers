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

    // --- Header: hidden on cover; show on scroll-up, hide on scroll-down ---
    var header = d3.select('.top-nav');
    header.classed('hidden', true);

    var pastCover = false;
    var lastScrollTop = 0;

    var coverEl = document.getElementById('top');
    if (coverEl) {
        var coverObserver = new IntersectionObserver(function(entries) {
            entries.forEach(function(entry) {
                pastCover = !entry.isIntersecting;
                if (!pastCover) header.classed('hidden', true);
            });
        }, { threshold: 0 });
        coverObserver.observe(coverEl);
    }

    window.addEventListener('scroll', function() {
        if (!pastCover) return;
        var scrollTop = window.pageYOffset || document.documentElement.scrollTop;
        header.classed('hidden', scrollTop > lastScrollTop);
        lastScrollTop = scrollTop <= 0 ? 0 : scrollTop;
    });

})();
