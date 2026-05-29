(function() {

    // --- Reveal animations ---
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

    // --- Side-nav dot tracking ---
    var sideNav = document.getElementById('side-nav');
    var sideItems = Array.from(document.querySelectorAll('.side-nav-item'));

    function setActive(href) {
        sideItems.forEach(function(item) {
            item.classList.toggle('active', item.getAttribute('href') === href);
        });
    }

    // Show side nav once user leaves the cover
    var coverEl = document.getElementById('top');
    if (coverEl) {
        new IntersectionObserver(function(entries) {
            entries.forEach(function(entry) {
                sideNav.classList.toggle('visible', !entry.isIntersecting);
                if (entry.isIntersecting) setActive('#top');
            });
        }, { threshold: 0 }).observe(coverEl);
    }

    // Track which section is in view. Sections are taller than the viewport, so a
    // fixed intersection ratio (e.g. 40% visible) can never be reached for the big
    // dashboard section. Instead, mark active the last section whose top edge has
    // scrolled above a reference line ~40% down the viewport.
    var targets = [
        { el: document.getElementById('duration'),          href: '#duration'          },
        { el: document.getElementById('genres'),            href: '#genres'            },
        { el: document.getElementById('dashboard-section'), href: '#dashboard-section' },
    ].filter(function(t) { return t.el; });

    function updateActiveSection() {
        var line = window.innerHeight * 0.4;
        var current = null;
        targets.forEach(function(t) {
            if (t.el.getBoundingClientRect().top <= line) current = t;
        });
        if (current) setActive(current.href);
    }

    window.addEventListener('scroll', updateActiveSection, { passive: true });
    window.addEventListener('resize', updateActiveSection);
    updateActiveSection();

})();
