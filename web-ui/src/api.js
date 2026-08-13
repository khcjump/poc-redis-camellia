// API client — all requests go to SAME-ORIGIN /cloud/* and /onprem/*
// (reverse-proxied by the Express server / Vite dev proxy). No CORS.

const BASE = {
  cloud: '/cloud',
  onprem: '/onprem',
}

async function http(method, region, path, { query, body } = {}) {
  const base = BASE[region]
  let url = `${base}${path}`
  if (query) {
    const qs = new URLSearchParams()
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined && v !== null && v !== '') qs.set(k, v)
    }
    const s = qs.toString()
    if (s) url += `?${s}`
  }
  const opts = { method, headers: {} }
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json'
    opts.body = JSON.stringify(body)
  }
  let res
  try {
    res = await fetch(url, opts)
  } catch (e) {
    // network error (backend down / proxy 502 not delivered as JSON)
    throw new ApiError(0, 'NETWORK_ERROR', `無法連線至 ${region} 後端`, e)
  }
  let data = null
  const text = await res.text()
  if (text) {
    try { data = JSON.parse(text) } catch { data = { raw: text } }
  }
  if (!res.ok) {
    const code = (data && (data.error || data.status)) || `HTTP_${res.status}`
    const msg = (data && (data.message || data.error || data.detail)) || res.statusText
    throw new ApiError(res.status, code, msg, data)
  }
  return data
}

export class ApiError extends Error {
  constructor(status, code, message, payload) {
    super(message)
    this.status = status
    this.code = code
    this.payload = payload
    this.name = 'ApiError'
  }
  get isTooFrequent() { return this.status === 429 }
}

export const api = {
  health: (region) => http('GET', region, '/api/health'),
  writeSession: (region, { key, value, ttlSeconds }) =>
    http('POST', region, '/api/session', { body: { key, value, ttlSeconds } }),
  compare: (region, key) => http('GET', region, '/api/compare', { query: { key } }),
  getSwitch: (region) => http('GET', region, '/api/switch'),
  switchTo: (region, to) => http('POST', region, '/api/switch', { query: { to } }),
  getParams: (region) => http('GET', region, '/api/params'),
  setParams: (region, body) => http('POST', region, '/api/params', { body }),
  queueStatus: (region) => http('GET', region, '/api/queue-status'),
}

// ---- polling hook ----
import { useEffect, useRef, useState } from 'react'

export function usePoll(fn, intervalMs, deps = []) {
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(true)
  const fnRef = useRef(fn)
  fnRef.current = fn
  const alive = useRef(true)

  useEffect(() => {
    alive.current = true
    let timer
    const run = async () => {
      try {
        const d = await fnRef.current()
        if (alive.current) { setData(d); setError(null) }
      } catch (e) {
        if (alive.current) setError(e)
      } finally {
        if (alive.current) { setLoading(false); timer = setTimeout(run, intervalMs) }
      }
    }
    run()
    return () => { alive.current = false; clearTimeout(timer) }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps)

  const refresh = async () => {
    try {
      const d = await fnRef.current()
      if (alive.current) { setData(d); setError(null) }
    } catch (e) { if (alive.current) setError(e) }
  }
  return { data, error, loading, refresh, setData }
}