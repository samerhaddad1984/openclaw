// OtoCPA portal service worker.
//
// Scope is '/' (see Service-Worker-Allowed header on the sw.js
// response) so one registration covers both /c/ and /cp/ portals.
// Strategy: network-first for navigation, fall back to the cached
// offline page when the network is unreachable.
//
// Keep this file small — it ships to every portal user on install
// and can't be minified by a bundler in production.

const CACHE_NAME = 'otocpa-portal-v1';
const OFFLINE_URL = '/c/offline';
const PRECACHE = [
  OFFLINE_URL,
  '/static/pwa/icon-192.png',
  '/static/pwa/icon-512.png',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(PRECACHE))
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) => Promise.all(
      keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k))
    )).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const req = event.request;

  // Only handle GETs. POST (uploads, invites, etc.) must go
  // straight to the network — queuing mutations would break the
  // accountant's trust that hitting "Send" == sent.
  if (req.method !== 'GET') {
    return;
  }

  // Navigation requests: network-first with offline fallback.
  if (req.mode === 'navigate') {
    event.respondWith(
      fetch(req).catch(() => caches.match(OFFLINE_URL))
    );
    return;
  }

  // Static PWA assets: cache-first (they rarely change, and the
  // dashboard serves them with Cache-Control already).
  const url = new URL(req.url);
  if (url.pathname.startsWith('/static/pwa/')) {
    event.respondWith(
      caches.match(req).then((cached) => cached || fetch(req))
    );
  }
});
