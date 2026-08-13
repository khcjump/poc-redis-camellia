import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Vite config — SPA build only. The Express server (server.js) handles
// runtime proxying in production; in dev, Vite serves the SPA on :5173
// and the same-origin /cloud /onprem prefixes are expected to be wired
// by the operator (e.g. run `node server.js` alongside, or use the
// docker-compose setup). For pure dev convenience we also forward the
// proxy prefixes to the dev server so `npm run dev` works standalone.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/cloud': {
        target: process.env.CLOUD_API || 'http://localhost:8080',
        changeOrigin: true,
        pathRewrite: { '^/cloud': '' },
      },
      '/onprem': {
        target: process.env.ONPREM_API || 'http://localhost:8081',
        changeOrigin: true,
        pathRewrite: { '^/onprem': '' },
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: false,
  },
})