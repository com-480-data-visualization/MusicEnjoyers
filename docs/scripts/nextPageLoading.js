// Page Navigation Script
// Handles auto-hide header and bottom scroll navigation

// Auto-hide header on scroll
let lastScrollTop = 0;
const header = document.querySelector('.top-nav');

window.addEventListener('scroll', () => {
    const scrollTop = window.pageYOffset || document.documentElement.scrollTop;

    if (scrollTop > lastScrollTop && scrollTop > 100) {
        // Scrolling down
        header.classList.add('hidden');
    } else {
        // Scrolling up
        header.classList.remove('hidden');
    }

    lastScrollTop = scrollTop <= 0 ? 0 : scrollTop;
});

// Bottom scroll to next page
let bottomScrollTimer;
let isAtBottom = false;
const loadingBar = document.getElementById('loadingBar');
const loadingProgress = document.getElementById('loadingProgress');

// Configuration for next page navigation
const pageConfig = {
    'duration.html': 'genres.html',
    'genres.html': 'dashboard.html',
    'dashboard.html': 'duration.html'
};

// Get current page filename
const currentPage = window.location.pathname.split('/').pop() || 'index.html';
const nextPage = pageConfig[currentPage] || 'duration.html';

function checkBottomScroll() {
    const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
    const windowHeight = window.innerHeight;
    const documentHeight = document.documentElement.scrollHeight;

    if (scrollTop + windowHeight >= documentHeight - 10) {
        if (!isAtBottom) {
            isAtBottom = true;
            startLoadingBar();
        }
    } else {
        if (isAtBottom) {
            isAtBottom = false;
            stopLoadingBar();
        }
    }
}

function startLoadingBar() {
    loadingBar.classList.add('visible');
    loadingProgress.classList.add('fill');

    bottomScrollTimer = setTimeout(() => {
        // Navigate to next page based on current page
        window.location.href = nextPage;
    }, 3000);
}

function stopLoadingBar() {
    loadingBar.classList.remove('visible');
    loadingProgress.classList.remove('fill');
    clearTimeout(bottomScrollTimer);
}

window.addEventListener('scroll', checkBottomScroll);