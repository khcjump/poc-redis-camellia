import React, { useState } from 'react'
import { Panel, Btn, Segmented, Alert, RegionTag, fmtTime, fmtMs } from './ui.jsx'
import { api } from '../api.js'

function HashCol({ region, data, loading, error }) {
  return (
    <div className={`cmp__col cmp__col--${region}`}>
      <div className="cmp__colhead">
        <RegionTag region={region} />
        {data && <span className="faint mono" style={{ fontSize: 11 }}>{data.exists ? 'EXISTS' : 'NOT FOUND'}</span>}
      </div>
      {loading ? (
        <div className="skeleton" style={{ height: 100 }} />
      ) : error ? (
        <Alert kind="bad">{error.message}</Alert>
      ) : !data || !data.exists || !data.fields || Object.keys(data.fields).length === 0 ? (
        <div className="empty-state">無 Hash 欄位資料 (Key not found or empty)</div>
      ) : (
        <div className="stack">
          {Object.entries(data.fields).map(([f, item]) => (
            <div key={f} className="kv" style={{ borderBottom: '1px dashed var(--border)', paddingBottom: 6 }}>
              <span className="kv__k">field</span>
              <span className="kv__v mono" style={{ color: 'var(--accent)' }}>{f}</span>
              <span className="kv__k">value</span>
              <span className="kv__v mono">{item.value}</span>
              <span className="kv__k">origin</span>
              <span className="kv__v">{item.origin || '—'}</span>
              <span className="kv__k">writtenAt</span>
              <span className="kv__v">{fmtTime(item.writtenAt)}</span>
              <span className="kv__k">lag</span>
              <span className="kv__v">{fmtMs(item.lagMs)}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default function HashPanel() {
  const [target, setTarget] = useState('cloud')
  const [key, setKey] = useState('demo:hash-001')
  const [field, setField] = useState('user:1')
  const [value, setValue] = useState('alice')
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState(null)

  // Compare section state
  const [queryKey, setQueryKey] = useState('demo:hash-001')
  const [cmpLoading, setCmpLoading] = useState(false)
  const [cloudData, setCloudData] = useState(null)
  const [onpremData, setOnpremData] = useState(null)
  const [cloudErr, setCloudErr] = useState(null)
  const [onpremErr, setOnpremErr] = useState(null)

  async function submitWrite(e) {
    e.preventDefault()
    if (!key.trim() || !field.trim()) return
    setBusy(true)
    setMsg(null)
    const body = { key: key.trim(), field: field.trim(), value: value.trim() }
    const targets = target === 'both' ? ['cloud', 'onprem'] : [target]
    try {
      await Promise.all(targets.map((r) => api.writeHash(r, body)))
      setMsg({ kind: 'ok', text: `Hash 寫入成功 (${targets.join(', ')})` })
      queryHash()
    } catch (err) {
      setMsg({ kind: 'bad', text: `寫入失敗：${err.message}` })
    } finally {
      setBusy(false)
    }
  }

  async function queryHash() {
    if (!queryKey.trim()) return
    setCmpLoading(true)
    setCloudErr(null); setOnpremErr(null)
    await Promise.all([
      api.getHash('cloud', queryKey.trim()).then(setCloudData).catch(setCloudErr),
      api.getHash('onprem', queryKey.trim()).then(setOnpremData).catch(setOnpremErr),
    ])
    setCmpLoading(false)
  }

  return (
    <Panel title="Hash 操作與跨區比對" en="Hash Operations & Cross-Region Sync">
      <form onSubmit={submitWrite} className="stack" style={{ marginBottom: 'var(--sp-4)' }}>
        <div className="field">
          <label className="field__label">寫入目標 target region</label>
          <Segmented
            value={target}
            onChange={setTarget}
            options={[
              { value: 'cloud', label: 'Cloud' },
              { value: 'onprem', label: 'OnPrem' },
              { value: 'both', label: '兩者 BOTH' },
            ]}
          />
        </div>

        <div className="form-row form-row--3">
          <div className="field">
            <label className="field__label">Hash Key</label>
            <input className="input" value={key} onChange={(e) => setKey(e.target.value)} placeholder="demo:hash-001" />
          </div>
          <div className="field">
            <label className="field__label">Field (HSET)</label>
            <input className="input" value={field} onChange={(e) => setField(e.target.value)} placeholder="user:1" />
          </div>
          <div className="field">
            <label className="field__label">Value</label>
            <input className="input" value={value} onChange={(e) => setValue(e.target.value)} placeholder="alice" />
          </div>
        </div>

        <div className="row row--between">
          <Btn type="submit" variant="primary" disabled={busy || !key.trim() || !field.trim()}>
            {busy ? '寫入中…' : '寫入 Hash (HSET)'}
          </Btn>
          {msg && <Alert kind={msg.kind}>{msg.text}</Alert>}
        </div>
      </form>

      <div className="divider" style={{ margin: 'var(--sp-3) 0' }} />

      <div className="stack">
        <div className="form-row form-row--2" style={{ alignItems: 'flex-end' }}>
          <div className="field">
            <label className="field__label">查詢 / 比對 Hash Key</label>
            <input className="input" value={queryKey} onChange={(e) => setQueryKey(e.target.value)} placeholder="demo:hash-001" />
          </div>
          <Btn variant="secondary" onClick={queryHash} disabled={cmpLoading || !queryKey.trim()}>
            {cmpLoading ? '查詢中…' : '查詢與比對 Query & Compare'}
          </Btn>
        </div>

        <div className="cmp">
          <HashCol region="cloud" data={cloudData} loading={cmpLoading} error={cloudErr} />
          <HashCol region="onprem" data={onpremData} loading={cmpLoading} error={onpremErr} />
        </div>
      </div>
    </Panel>
  )
}
