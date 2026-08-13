import React, { useState } from 'react'
import { Panel, Btn, Alert, RegionTag, fmtTime, fmtMs } from './ui.jsx'
import { api, ApiError } from '../api.js'

const LAG_PROPAGATING_MS = 2000 // lag above this while one side missing => "propagating"

function verdictFor(cloud, onprem) {
  if (!cloud && !onprem) return { kind: 'neutral', label: '尚未比對', sub: '輸入 key 後執行比對' }
  const cErr = cloud && cloud.error
  const oErr = onprem && onprem.error
  if (cErr && oErr) return { kind: 'red', label: '無法比對', sub: '兩側後端皆錯誤' }

  const cExists = cloud && !cErr && cloud.exists
  const oExists = onprem && !oErr && onprem.exists
  const cVal = cloud && !cErr ? cloud.value : undefined
  const oVal = onprem && !oErr ? onprem.value : undefined

  if (cExists && oExists) {
    if (String(cVal) === String(oVal)) {
      const maxLag = Math.max(cloud.lagMs || 0, onprem.lagMs || 0)
      return { kind: 'green', label: '一致 (consistent)', sub: `max lag ${fmtMs(maxLag)}` }
    }
    return { kind: 'red', label: '不一致 (inconsistent)', sub: '兩側 value 不同' }
  }
  // one or both missing
  const missing = !cExists ? 'Cloud' : 'OnPrem'
  const otherLag = cExists ? cloud.lagMs : oExists ? onprem.lagMs : null
  if (otherLag != null && otherLag < LAG_PROPAGATING_MS) {
    return { kind: 'yellow', label: '延遲中 (propagating)', sub: `${missing} 尚未收到，lag ${fmtMs(otherLag)}` }
  }
  return { kind: 'yellow', label: '延遲中 (propagating)', sub: `${missing} 不存在此 key` }
}

export default function ComparePanel({ onLatency }) {
  const [key, setKey] = useState('')
  const [busy, setBusy] = useState(false)
  const [cloud, setCloud] = useState(null) // {data?, error?}
  const [onprem, setOnprem] = useState(null)

  async function run(e) {
    e?.preventDefault()
    if (!key.trim()) return
    setBusy(true)
    setCloud(null); setOnprem(null)
    const k = key.trim()
    const [c, o] = await Promise.all([
      api.compare('cloud', k).then((d) => ({ data: d })).catch((err) => ({ error: err })),
      api.compare('onprem', k).then((d) => ({ data: d })).catch((err) => ({ error: err })),
    ])
    setCloud(c); setOnprem(o)
    // latency readout: max lag of sides that exist
    const lags = [c, o].filter((x) => x.data && x.data.exists).map((x) => x.data.lagMs || 0)
    const lat = lags.length ? Math.max(...lags) : null
    onLatency && onLatency(lat)
    setBusy(false)
  }

  const verdict = verdictFor(cloud, onprem)

  return (
    <Panel
      title="一致性比對"
      en="Sync Compare"
      right={
        <span className="lat">
          <span className="lat__num">{
            (cloud?.data?.exists || onprem?.data?.exists)
              ? fmtMs(Math.max(cloud?.data?.lagMs || 0, onprem?.data?.lagMs || 0))
              : '—'
          }</span>
          <span className="lat__unit">sync latency</span>
        </span>
      }
    >
      <form onSubmit={run} className="stack">
        <div className="form-row form-row--2" style={{ gridTemplateColumns: '2fr 1fr', alignItems: 'end' }}>
          <div className="field">
            <label className="field__label">key</label>
            <input className="input" value={key} onChange={(e) => setKey(e.target.value)} placeholder="demo:key-001" />
          </div>
          <Btn type="submit" variant="primary" disabled={busy || !key.trim()}>
            {busy ? '比對中…' : '比對 Compare'}
          </Btn>
        </div>

        {verdict && (
          <div className={`verdict verdict--${verdict.kind}`}>
            <span className="verdict__label">{verdict.label}</span>
            <span className="verdict__sub">{verdict.sub}</span>
          </div>
        )}

        <div className="cmp">
          <CompareCol region="cloud" state={cloud} />
          <CompareCol region="onprem" state={onprem} />
        </div>
      </form>
    </Panel>
  )
}

function CompareCol({ region, state }) {
  return (
    <div className={`cmp__col cmp__col--${region}`}>
      <div className="cmp__colhead">
        <RegionTag region={region} />
        {state && (state.error ? (
          <span className="alert alert--bad" style={{ padding: '2px 8px' }}>錯誤</span>
        ) : state.data && state.data.exists ? (
          <span className="alert alert--ok" style={{ padding: '2px 8px' }}>存在</span>
        ) : state.data ? (
          <span className="alert alert--warn" style={{ padding: '2px 8px' }}>不存在</span>
        ) : null)}
      </div>
      {!state ? (
        <div className="cmp__missing">尚未查詢</div>
      ) : state.error ? (
        <Alert kind="bad">{state.error.message}</Alert>
      ) : !state.data ? (
        <div className="cmp__missing">無資料</div>
      ) : !state.data.exists ? (
        <div className="cmp__missing">此 key 不存在</div>
      ) : (
        <div className="kv">
          <span className="kv__k">value</span>
          <span className="kv__v">{state.data.value ?? '—'}</span>
          <span className="kv__k">origin</span>
          <span className="kv__v">{state.data.origin || '—'}</span>
          <span className="kv__k">writtenAt</span>
          <span className="kv__v">{fmtTime(state.data.writtenAt)}</span>
          <span className="kv__k">lagMs</span>
          <span className="kv__v" style={{ color: 'var(--info)' }}>{fmtMs(state.data.lagMs)}</span>
          <span className="kv__k">ttl</span>
          <span className="kv__v">{state.data.ttlSeconds != null ? `${state.data.ttlSeconds}s` : '—'}</span>
        </div>
      )}
    </div>
  )
}