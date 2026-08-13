import React, { useState } from 'react'
import { Panel, Btn, Segmented, Alert, RegionTag, fmtTime } from './ui.jsx'
import { api, ApiError } from '../api.js'

export default function WriteSession() {
  const [target, setTarget] = useState('cloud') // 'cloud' | 'onprem' | 'both'
  const [key, setKey] = useState('')
  const [value, setValue] = useState('')
  const [ttl, setTtl] = useState('')
  const [busy, setBusy] = useState(false)
  const [result, setResult] = useState(null) // {cloud?, onprem?, error?}

  async function submit(e) {
    e.preventDefault()
    if (!key.trim()) return
    setBusy(true)
    setResult(null)
    const body = { key: key.trim(), value: value.trim(), ttlSeconds: ttl ? Number(ttl) : undefined }
    const targets = target === 'both' ? ['cloud', 'onprem'] : [target]
    const out = {}
    await Promise.all(
      targets.map(async (r) => {
        try {
          out[r] = await api.writeSession(r, body)
        } catch (e) {
          out[r] = { error: e }
        }
      })
    )
    setResult(out)
    setBusy(false)
  }

  return (
    <Panel
      title="寫入 Session"
      en="Write Session"
      right={<RegionTag region={target === 'both' ? 'cloud' : target} />}
    >
      <form onSubmit={submit} className="stack">
        <div className="field">
          <label className="field__label">目標區域 target region</label>
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
            <label className="field__label">key</label>
            <input className="input" value={key} onChange={(e) => setKey(e.target.value)} placeholder="demo:key-001" />
          </div>
          <div className="field">
            <label className="field__label">value</label>
            <input className="input" value={value} onChange={(e) => setValue(e.target.value)} placeholder="hello" />
          </div>
          <div className="field">
            <label className="field__label">ttlSeconds (optional)</label>
            <input className="input" value={ttl} onChange={(e) => setTtl(e.target.value)} placeholder="60" inputMode="numeric" />
          </div>
        </div>

        <div className="row row--between">
          <Btn type="submit" variant="primary" disabled={busy || !key.trim()}>
            {busy ? '寫入中…' : '寫入 Write'}
          </Btn>
          <span className="faint mono" style={{ fontSize: 11 }}>
            {target === 'both' ? '同時寫入兩區（雙向同步示範）' : `寫入至 ${target}`}
          </span>
        </div>

        {result && (
          <div className="stack">
            {['cloud', 'onprem'].map((r) =>
              result[r] ? (
                <div key={r} className="cmp__col cmp__col--cloud" style={{ borderLeftColor: r === 'cloud' ? 'var(--cloud)' : 'var(--onprem)' }}>
                  <div className="cmp__colhead">
                    <RegionTag region={r} />
                    {result[r].error ? (
                      <span className="alert alert--bad" style={{ padding: '2px 8px' }}>失敗</span>
                    ) : (
                      <span className="alert alert--ok" style={{ padding: '2px 8px' }}>成功</span>
                    )}
                  </div>
                  {result[r].error ? (
                    <Alert kind="bad">{result[r].error.message}</Alert>
                  ) : (
                    <div className="kv">
                      <span className="kv__k">origin</span>
                      <span className="kv__v">{result[r].origin || '—'}</span>
                      <span className="kv__k">writtenAt</span>
                      <span className="kv__v">{fmtTime(result[r].writtenAt)}</span>
                      <span className="kv__k">ttl</span>
                      <span className="kv__v">{result[r].ttlSeconds != null ? `${result[r].ttlSeconds}s` : '—'}</span>
                    </div>
                  )}
                </div>
              ) : null
            )}
          </div>
        )}
      </form>
    </Panel>
  )
}