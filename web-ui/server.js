// Camellia Sync Console — Express server
// Serves the built SPA (dist/) and reverse-proxies same-origin /cloud/*
// and /onprem/* to CLOUD_API / ONPREM_API (runtime env). No CORS.
import express from 'express'
import { createProxyMiddleware } from 'http-proxy-middleware'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import fs from 'node:fs'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const PORT = process.env.PORT || 3000
const CLOUD_API = process.env.CLOUD_API || 'http://localhost:8080'
const ONPREM_API = process.env.ONPREM_API || 'http://localhost:8081'

const app = express()

// --- reverse proxies (same-origin -> backend, no CORS) ---
// /cloud/api/health  ->  CLOUD_API/api/health
app.use(
  '/cloud',
  createProxyMiddleware({
    target: CLOUD_API,
    changeOrigin: true,
    pathRewrite: { '^/cloud': '' },
    logLevel: 'warn',
    onError: (err, req, res) => {
      if (res && !res.headersSent) {
        res.status(502).json({
          status: 'DOWN',
          error: 'cloud backend unreachable',
          detail: err && err.message ? err.message : String(err),
        })
      }
    },
  })
)

app.use(
  '/onprem',
  createProxyMiddleware({
    target: ONPREM_API,
    changeOrigin: true,
    pathRewrite: { '^/onprem': '' },
    logLevel: 'warn',
    onError: (err, req, res) => {
      if (res && !res.headersSent) {
        res.status(502).json({
          status: 'DOWN',
          error: 'onprem backend unreachable',
          detail: err && err.message ? err.message : String(err),
        })
      }
    },
  })
)

// --- health endpoint for the console itself ---
app.get('/__console/health', (_req, res) => {
  res.json({ status: 'UP', port: PORT, CLOUD_API, ONPREM_API })
})

// --- serve built SPA ---
const distDir = path.join(__dirname, 'dist')
if (fs.existsSync(distDir)) {
  app.use(express.static(distDir))
  // SPA fallback
  app.get('*', (req, res, next) => {
    // don't shadow proxy/api routes
    if (req.path.startsWith('/cloud') || req.path.startsWith('/onprem') || req.path.startsWith('/__console')) {
      return next()
    }
    res.sendFile(path.join(distDir, 'index.html'))
  })
} else {
  app.get('/', (_req, res) =>
    res
      .status(503)
      .send('dist/ not built. Run `npm run build` first.')
  )
}

app.listen(PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`[camellia-sync-console] listening on :${PORT}`)
  console.log(`  CLOUD_API  = ${CLOUD_API}`)
  console.log(`  ONPREM_API = ${ONPREM_API}`)
})