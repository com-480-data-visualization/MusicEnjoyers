const metrics = ["Duration", "BPM", "Loudness", "Danceability", "Speechiness", "Acousticness", "Energy"];
const COUNT = metrics.length;
const MIN_DEG = -145, MAX_DEG = 145;
let currentIndex = 0, isDragging = false, startY = 0, startIndex = 0;

const knob = document.getElementById('knob');
const knobBody = document.getElementById('knobBody');
const tickRing = document.getElementById('tickRing');
const display = document.getElementById('metricDisplay');

function indexToDeg(i) {
    return MIN_DEG + (i / (COUNT - 1)) * (MAX_DEG - MIN_DEG);
}

function buildTicks() {
    tickRing.innerHTML = '';
    const r = 76, cx = 80, cy = 80;
    metrics.forEach((m, i) => {
        const deg = indexToDeg(i);
        const rad = (deg - 90) * Math.PI / 180;
        const tx = cx + (r + 16) * Math.cos(rad);
        const ty = cy + (r + 16) * Math.sin(rad);

        const tick = document.createElement('div');
        tick.id = 'tick-' + i;
        tick.style.cssText = `position:absolute;top:50%;left:50%;width:2px;height:8px;border-radius:1px;background:#3a3a3a;transform-origin:bottom center;transform:translateX(-50%) translateY(-${r}px) rotate(${deg}deg);`;
        tickRing.appendChild(tick);

        const lbl = document.createElement('div');
        lbl.className = 'tick-label';
        lbl.id = 'lbl-' + i;
        lbl.textContent = m.length > 6 ? m.slice(0, 5) : m;
        lbl.style.cssText = `left:${tx}px;top:${ty}px;transform:translate(-50%,-50%);`;
        tickRing.appendChild(lbl);
    });
}

function updateKnob(idx) {
    const deg = indexToDeg(idx);
    knobBody.style.transform = `rotate(${deg}deg)`;
    display.textContent = metrics[idx];
    for (let i = 0; i < COUNT; i++) {
        const lbl = document.getElementById('lbl-' + i);
        const tick = document.getElementById('tick-' + i);
        if (lbl) lbl.className = 'tick-label' + (i === idx ? ' active-label' : '');
        if (tick) {
            tick.style.background = i === idx ? '#ff6b35' : '#3a3a3a';
            tick.style.height = i === idx ? '10px' : '8px';
        }
    }
    knob.dispatchEvent(new CustomEvent('metric-change', {
        detail: { metric: metrics[idx].toLowerCase() },
        bubbles: true
    }));
}

buildTicks();
updateKnob(0);

knob.addEventListener('mousedown', e => {
    isDragging = true; startY = e.clientY; startIndex = currentIndex; e.preventDefault();
});
document.addEventListener('mousemove', e => {
    if (!isDragging) return;
    const next = Math.max(0, Math.min(COUNT - 1, startIndex + Math.round((startY - e.clientY) / 28)));
    if (next !== currentIndex) { currentIndex = next; updateKnob(next); }
});
document.addEventListener('mouseup', () => { isDragging = false; });

knob.addEventListener('wheel', e => {
    e.preventDefault();
    const next = Math.max(0, Math.min(COUNT - 1, currentIndex + (e.deltaY > 0 ? 1 : -1)));
    if (next !== currentIndex) { currentIndex = next; updateKnob(next); }
}, { passive: false });

knob.addEventListener('click', e => {
    if (Math.abs(startY - e.clientY) < 4) {
        currentIndex = (currentIndex + 1) % COUNT;
        updateKnob(currentIndex);
    }
});

let touchStartY = 0, touchStartIdx = 0;
knob.addEventListener('touchstart', e => {
    touchStartY = e.touches[0].clientY; touchStartIdx = currentIndex; e.preventDefault();
}, { passive: false });
knob.addEventListener('touchmove', e => {
    const next = Math.max(0, Math.min(COUNT - 1, touchStartIdx + Math.round((touchStartY - e.touches[0].clientY) / 28)));
    if (next !== currentIndex) { currentIndex = next; updateKnob(next); }
    e.preventDefault();
}, { passive: false });

// --- Visibility: knob is only shown while the dashboard or heatmap is on screen.
// Tracks each section's intersection state and toggles .visible accordingly. ---
const metricControl = document.getElementById('metric-control');
const knobTargets = ['dashboard-section', 'heatmap-section']
    .map(id => document.getElementById(id))
    .filter(Boolean);

if (metricControl && knobTargets.length) {
    const visibleSections = new Set();
    const obs = new IntersectionObserver(entries => {
        for (const entry of entries) {
            if (entry.isIntersecting) visibleSections.add(entry.target);
            else                      visibleSections.delete(entry.target);
        }
        metricControl.classList.toggle('visible', visibleSections.size > 0);
        metricControl.setAttribute('aria-hidden', visibleSections.size === 0);
    }, { threshold: 0 });
    knobTargets.forEach(t => obs.observe(t));
}