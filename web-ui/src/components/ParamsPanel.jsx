import React, { useState, useEffect } from 'react'
import { Panel, Btn, Alert, RegionTag } from './ui.jsx'
import { api, ApiError, usePoll } from '../api.js'

const POLL_MS = 5000

function ParamsCol({ region }) {
  const { data, refresh } = usePoll(() => api.getParams(region), POLL_MS)
  const [ttl, setTtl] = useState('')
  const [minInt, setMinInt] = useState('')
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState(null)

  useEffect(() => {
    if (data) {
      setTtl(data.queueTtlSeconds != null ? String(data.queueTtlSeconds) : '')
      setMinInt(data.minIntervalSeconds != null ? String(data.minIntervalSeconds) : '')
    }
  }, [data])

  async function save() {
    setBusy(true); setMsg(null)
    const body = {}
    if (ttl.trim() !== '') body.queueTtlSeconds = Number(ttl)
    if (minInt.trim() !== '') body.minIntervalSeconds = Number(minInt)
    try {
      await api.setParams(region, body)
      setMsg({ kind: 'ok', text: '參數已更新' })
      refresh()
    } catch (e) {
      setMsg({ kind: 'bad', text: `更新失敗：${e.message}` })
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className={`cmp__col cmp__col--${region}`}>
      <div className="cmp__colhead">
        <RegionTag region={region} />
        <span className="faint mono" style={{ fontSize: 11 }}>params</span>
      </div>
      {!data ? (
        <div className="skeleton" style={{ height: 120 }} />
      ) : (
        <div className="stack">
          <div className="kv">
            <span className="kv__k">role</span>
            <span className="kv__v">{data.role || '—'}</span>
            <span className="kv__k">location</span>
            <span className="kv__v">{data.location || '—'}</span>
            <span className="kv__k">producerQueue</span>
            <span className="kv__v">{data.producerQueueType || '—'}</span>
            <span className="kv__k">consumerQueue</span>
            <span className="kv__v">{data.consumerQueueType || '—'}</span>
            <span className="kv__k">redisMode</span>
            <span className="kv__v">{data.redisMode || '—'}</span>
          </div>

          <div className="form-row form-row--2">
            <div className="field">
              <label className="field__label">queueTtlSeconds</label>
              <input className="input" value={ttl} onChange={(e) => setTtl(e.target.value)} inputMode="numeric" />
            </div>
            <div className="field">
              <label className="field__label">minIntervalSeconds</label>
              <input className="input" value={minInt} onChange={(e) => setMinInt(e.target.value)} inputMode="numeric" />
            </div>
          </div>

          <Btn variant={region} size="sm" disabled={busy} onClick={save}>
            {busy ? '儲存中…' : '儲存 Save'}
          </Btn>

          {msg && <Alert kind={msg.kind}>{msg.text}</Alert>}
        </div>
      )}
    </div>
  )
}

export default function ParamsPanel() {
  return (
    <Panel title="執行參數" en="Runtime Params" right={<span className="meta-pill">poll 5s</span>}>
      <div className="cmp">
        <ParamsCol region="cloud" />
        <ParamsCol region="onprem" />
      </div>
    </Panel>
  )
}