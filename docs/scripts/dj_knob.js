const metrics = ["Duration", "BPM", "Loudness", "Danceability", "Speechiness", "Acousticness", "Energy"];
const COUNT = metrics.length;
const MIN_DEG = -145, MAX_DEG = 145;
let currentIndex = 0, isDragging = false, startY = 0;

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

// Map the pointer's angle around the knob centre to a metric index. 0° points
// straight up and clockwise is positive — the same convention as indexToDeg()
// and the CSS rotation — so rotating right advances through the metrics in order.
function pointerToIndex(clientX, clientY) {
    const rect = knob.getBoundingClientRect();
    const cx = rect.left + rect.width / 2;
    const cy = rect.top + rect.height / 2;
    let deg = Math.atan2(clientX - cx, -(clientY - cy)) * 180 / Math.PI;
    deg = Math.max(MIN_DEG, Math.min(MAX_DEG, deg));
    const idx = Math.round((deg - MIN_DEG) / (MAX_DEG - MIN_DEG) * (COUNT - 1));
    return Math.max(0, Math.min(COUNT - 1, idx));
}

let startX = 0, dragMoved = false;

function beginDrag(x, y) { isDragging = true; dragMoved = false; startX = x; startY = y; }
function moveDrag(x, y) {
    if (!isDragging) return;
    if (Math.abs(x - startX) > 3 || Math.abs(y - startY) > 3) dragMoved = true;
    const next = pointerToIndex(x, y);
    if (next !== currentIndex) { currentIndex = next; updateKnob(next); }
}

knob.addEventListener('mousedown', e => { beginDrag(e.clientX, e.clientY); e.preventDefault(); });
document.addEventListener('mousemove', e => moveDrag(e.clientX, e.clientY));
document.addEventListener('mouseup', () => { isDragging = false; });

knob.addEventListener('wheel', e => {
    e.preventDefault();
    const next = Math.max(0, Math.min(COUNT - 1, currentIndex + (e.deltaY > 0 ? 1 : -1)));
    if (next !== currentIndex) { currentIndex = next; updateKnob(next); }
}, { passive: false });

knob.addEventListener('click', () => {
    // A tap (no drag) advances one step; a drag already set the value.
    if (!dragMoved) {
        currentIndex = (currentIndex + 1) % COUNT;
        updateKnob(currentIndex);
    }
});

knob.addEventListener('touchstart', e => {
    beginDrag(e.touches[0].clientX, e.touches[0].clientY); e.preventDefault();
}, { passive: false });
knob.addEventListener('touchmove', e => {
    moveDrag(e.touches[0].clientX, e.touches[0].clientY); e.preventDefault();
}, { passive: false });
knob.addEventListener('touchend', () => { isDragging = false; });

// --- Visibility: knob is only shown while the dashboard or heatmap is on screen.
// Tracks each section's intersection state and toggles .visible accordingly. ---
const metricControl = document.getElementById('metric-control');
const knobTargets = ['dashboard-section']
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
    }, { threshold: 0.15 });
    knobTargets.forEach(t => obs.observe(t));
}