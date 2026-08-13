import React, { useEffect, useState, useRef } from 'react'
import { Panel, Btn, Alert, RegionTag, fmtTime } from './ui.jsx'
import { api, ApiError, usePoll } from '../api.js'

const POLL_MS = 3000

export default function SwitchPanel() {
  const { data: cloudSw, refresh: refreshCloud } = usePoll(() => api.getSwitch('cloud'), POLL_MS)
  const { data: onpremSw, refresh: refreshOnprem } = usePoll(() => api.getSwitch('onprem'), POLL_MS)

  const minInterval = (cloudSw && cloudSw.minIntervalSeconds) || 10
  const [cooldownUntil, setCooldownUntil] = useState(0)
  const [now, setNow] = useState(Date.now())
  const [busy, setBusy] = useState(null) // 'cloud' | 'onprem' | null
  const [msg, setMsg] = useState(null) // {kind, text, region}

  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 250)
    return () => clearInterval(t)
  }, [])

  const cooling = now < cooldownUntil
  const cooldownLeft = Math.max(0, (cooldownUntil - now) / 1000)
  const cooldownPct = cooling ? (cooldownLeft / minInterval) * 100 : 0

  async function doSwitch(region, to) {
    if (busy || cooling) return
    setBusy(region)
    setMsg(null)
    try {
      await api.switchTo(region, to)
      setMsg({ kind: 'ok', region, text: `已切換 ${region === 'cloud' ? '雲端' : '地端'} primary → ${to}` })
      setCooldownUntil(Date.now() + minInterval * 1000)
      refreshCloud(); refreshOnprem()
    } catch (e) {
      if (e instanceof ApiError && e.isTooFrequent) {
        setMsg({ kind: 'warn', region, text: `切換過於頻繁：需間隔 ${minInterval}s（429 Too Soon）` })
        setCooldownUntil(Date.now() + minInterval * 1000)
      } else {
        setMsg({ kind: 'bad', region, text: `切換失敗：${e.message}` })
      }
    } finally {
      setBusy(null)
    }
  }

  return (
    <Panel title="大小網切換" en="Primary Switch" right={<span className="meta-pill">minInterval {minInterval}s</span>}>
      <div className="stack">
        <div className="grid" style={{ gridTemplateColumns: '1fr 1fr', gap: 'var(--sp-3)' }}>
          <SwitchStateCard region="cloud" data={cloudSw} />
          <SwitchStateCard region="onprem" data={onpremSw} />
        </div>

        <div className="row row--wrap" style={{ gap: 'var(--sp-2)' }}>
          <Btn variant="cloud" disabled={busy != null || cooling} onClick={() => doSwitch('cloud', 'Cloud')}>
            {busy === 'cloud' ? '切換中…' : '切到 Cloud'}
          </Btn>
          <Btn variant="onprem" disabled={busy != null || cooling} onClick={() => doSwitch('onprem', 'OnPrem')}>
            {busy === 'onprem' ? '切換中…' : '切到 OnPrem'}
          </Btn>
          <span className="faint mono" style={{ fontSize: 11, alignSelf: 'center' }}>
            {cooling ? `冷卻中 ${cooldownLeft.toFixed(1)}s` : '可切換'}
          </span>
        </div>

        <div className="cool"><div className="cool__bar" style={{ width: `${cooling ? cooldownPct : 0}%` }} /></div>

        {msg && <Alert kind={msg.kind}>{msg.text}</Alert>}
      </div>
    </Panel>
  )
}

function SwitchStateCard({ region, data }) {
  return (
    <div className={`cmp__col cmp__col--${region}`}>
      <div className="cmp__colhead">
        <RegionTag region={region} />
        <span className="faint mono" style={{ fontSize: 11 }}>switch state</span>
      </div>
      {!data ? (
        <div className="skeleton" style={{ height: 48 }} />
      ) : (
        <div className="kv">
          <span className="kv__k">primary</span>
          <span className="kv__v" style={{ color: data.primary === 'Cloud' ? 'var(--cloud-bright)' : 'var(--onprem-bright)' }}>
            {data.primary || '—'}
          </span>
          <span className="kv__k">location</span>
          <span className="kv__v">{data.location || '—'}</span>
          <span className="kv__k">lastSwitchAt</span>
          <span className="kv__v">{fmtTime(data.lastSwitchAt)}</span>
        </div>
      )}
    </div>
  )
}