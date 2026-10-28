// API base URL. Order: runtime config (/config.js, written when the
// container starts), build-time VITE_API_URL, then the page's own origin
// (production: nginx proxies /api, /stats and /ws to the faster service).
const runtime = (window as any).__FASTER__?.apiUrl as string | undefined;

export const API: string = (runtime || import.meta.env.VITE_API_URL || window.location.origin).replace(/\/$/, '');

export const WS_URL: string = API.replace(/^http/, 'ws') + '/ws';
