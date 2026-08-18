import React, { useState } from 'react'
import { Panel, Btn, Segmented, Alert, RegionTag, fmtTime, fmtMs } from './ui.jsx'
import { api } from '../api.js'

function ZSetCol({ region, data, loading, error }) {
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
      ) : !data || !data.exists || !data.members || data.members.length === 0 ? (
        <div className="empty-state">無 ZSet 成員資料 (Key not found or empty)</div>
      ) : (
        <div className="stack">
          {data.members.map((item, idx) => (
            <div key={idx} className="kv" style={{ borderBottom: '1px dashed var(--border)', paddingBottom: 6 }}>
              <span className="kv__k">member</span>
              <span className="kv__v mono" style={{ color: 'var(--accent)' }}>{item.member}</span>
              <span className="kv__k">score</span>
              <span className="kv__v mono">{item.score}</span>
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

export default function ZSetPanel() {
  const [target, setTarget] = useState('cloud')
  const [key, setKey] = useState('demo:zset-001')
  const [member, setMember] = useState('player-1')
  const [score, setScore] = useState('100')
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState(null)

  // Compare section state
  const [queryKey, setQueryKey] = useState('demo:zset-001')
  const [cmpLoading, setCmpLoading] = useState(false)
  const [cloudData, setCloudData] = useState(null)
  const [onpremData, setOnpremData] = useState(null)
  const [cloudErr, setCloudErr] = useState(null)
  const [onpremErr, setOnpremErr] = useState(null)

  async function submitWrite(e) {
    e.preventDefault()
    if (!key.trim() || !member.trim()) return
    setBusy(true)
    setMsg(null)
    const body = { key: key.trim(), member: member.trim(), score: score ? Number(score) : 0 }
    const targets = target === 'both' ? ['cloud', 'onprem'] : [target]
    try {
      await Promise.all(targets.map((r) => api.writeZSet(r, body)))
      setMsg({ kind: 'ok', text: `ZSet 寫入成功 (${targets.join(', ')})` })
      queryZSet()
    } catch (err) {
      setMsg({ kind: 'bad', text: `寫入失敗：${err.message}` })
    } finally {
      setBusy(false)
    }
  }

  async function queryZSet() {
    if (!queryKey.trim()) return
    setCmpLoading(true)
    setCloudErr(null); setOnpremErr(null)
    await Promise.all([
      api.getZSet('cloud', queryKey.trim()).then(setCloudData).catch(setCloudErr),
      api.getZSet('onprem', queryKey.trim()).then(setOnpremData).catch(setOnpremErr),
    ])
    setCmpLoading(false)
  }

  return (
    <Panel title="Sorted Set (ZSet) 操作與跨區比對" en="ZSet Operations & Cross-Region Sync">
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
            <label className="field__label">ZSet Key</label>
            <input className="input" value={key} onChange={(e) => setKey(e.target.value)} placeholder="demo:zset-001" />
          </div>
          <div className="field">
            <label className="field__label">Member (ZADD)</label>
            <input className="input" value={member} onChange={(e) => setMember(e.target.value)} placeholder="player-1" />
          </div>
          <div className="field">
            <label className="field__label">Score</label>
            <input className="input" value={score} onChange={(e) => setScore(e.target.value)} placeholder="100" inputMode="numeric" />
          </div>
        </div>

        <div className="row row--between">
          <Btn type="submit" variant="primary" disabled={busy || !key.trim() || !member.trim()}>
            {busy ? '寫入中…' : '寫入 ZSet (ZADD)'}
          </Btn>
          {msg && <Alert kind={msg.kind}>{msg.text}</Alert>}
        </div>
      </form>

      <div className="divider" style={{ margin: 'var(--sp-3) 0' }} />

      <div className="stack">
        <div className="form-row form-row--2" style={{ alignItems: 'flex-end' }}>
          <div className="field">
            <label className="field__label">查詢 / 比對 ZSet Key</label>
            <input className="input" value={queryKey} onChange={(e) => setQueryKey(e.target.value)} placeholder="demo:zset-001" />
          </div>
          <Btn variant="secondary" onClick={queryZSet} disabled={cmpLoading || !queryKey.trim()}>
            {cmpLoading ? '查詢中…' : '查詢與比對 Query & Compare'}
          </Btn>
        </div>

        <div className="cmp">
          <ZSetCol region="cloud" data={cloudData} loading={cmpLoading} error={cloudErr} />
          <ZSetCol region="onprem" data={onpremData} loading={cmpLoading} error={onpremErr} />
        </div>
      </div>
    </Panel>
  )
}
