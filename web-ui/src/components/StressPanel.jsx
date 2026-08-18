import React, { useEffect, useRef, useState } from 'react'
import { Panel, Btn, Segmented, Alert, fmtMs, fmtNum, fmtTime } from './ui.jsx'
import { api } from '../api.js'

// ---- stress-test constants ----
const SIZES = [10, 100, 1000]
const TTL_SECONDS = 300          // stress keys self-expire; avoids stale test data
const SUBMIT_CONCURRENCY = 20    // max parallel POSTs in 'concurrent' mode
const CONFIRM_CONCURRENCY = 64   // max parallel compare() during drain poll
const DRAIN_POLL_MS = 250        // poll interval while waiting for remote confirmation
const DRAIN_TIMEOUT_MS = 120000  // give up waiting for full confirmation

const PHASE_LABEL = {
  idle: '閒置 idle',
  submitting: '寫入中 submitting',
  draining: '等待非同步同步 draining',
  done: '完成 done',
  'done-errors': '完成（部分寫入失敗）',
  timeout: '同步逾時 timeout',
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

/**
 * 非同步更新壓力測試：從來源區域送出 10/100/1000 筆 session 寫入，
 * 量測非同步同步的耗時差異（submit = 同步寫入、drain = 對端確認全部到達）。
 * 純前端、無後端變更；重用 api.writeSession / api.compare / api.queueStatus。
 */
export default function StressPanel() {
  const [source, setSource] = useState('cloud')     // which region we WRITE to
  const [size, setSize] = useState(10)
  const [mode, setMode] = useState('sequential')    // 'sequential' | 'concurrent'
  const [dataType, setDataType] = useState('session') // 'session' | 'hash' | 'zset'
  const [running, setRunning] = useState(false)
  const [phase, setPhase] = useState('idle')
  const [submitted, setSubmitted] = useState(0)
  const [confirmed, setConfirmed] = useState(0)
  const [submitErrors, setSubmitErrors] = useState(0)
  const [results, setResults] = useState([])

  const cancelRef = useRef(false)

  useEffect(() => () => { cancelRef.current = true }, [])

  const remote = source === 'cloud' ? 'onprem' : 'cloud'

  async function run() {
    if (running) return
    cancelRef.current = false
    setRunning(true)
    setPhase('submitting')
    setSubmitted(0)
    setConfirmed(0)
    setSubmitErrors(0)

    const id = `${Date.now()}-${Math.random().toString(36).slice(2, 6)}`
    const prefix = `stress.${dataType}.${id}`
    const submitTs = new Array(size).fill(null)  // browser wall-clock at each successful submit
    let errs = 0
    const t0 = performance.now()

    // snapshot remote queue metrics for replaySuccess delta cross-check
    let qBefore = null
    try { qBefore = await api.queueStatus(remote) } catch { /* cross-check is best-effort */ }

    // ---- submit phase (synchronous writes to SOURCE region) ----
    const writeOne = async (i) => {
      try {
        if (dataType === 'hash') {
          await api.writeHash(source, { key: `${prefix}.${i}`, field: `field_${i}`, value: `v${id}.${i}` })
        } else if (dataType === 'zset') {
          await api.writeZSet(source, { key: `${prefix}.${i}`, member: `member_${i}`, score: i * 1.5 })
        } else {
          await api.writeSession(source, { key: `${prefix}.${i}`, value: `v${id}.${i}`, ttlSeconds: TTL_SECONDS })
        }
        submitTs[i] = Date.now()
      } catch {
        errs += 1
        setSubmitErrors(errs)
      }
      setSubmitted((s) => s + 1)
    }

    if (mode === 'sequential') {
      for (let i = 0; i < size; i++) {
        if (cancelRef.current) return
        await writeOne(i)
      }
    } else {
      let next = 0
      const worker = async () => {
        while (true) {
          if (cancelRef.current) return
          const i = next++
          if (i >= size) return
          await writeOne(i)
        }
      }
      await Promise.all(Array.from({ length: Math.min(SUBMIT_CONCURRENCY, size) }, worker))
    }

    if (cancelRef.current) return
    const tSubmitEnd = performance.now()
    setPhase('draining')

    // ---- drain phase (async propagation: remote confirms all N) ----
    const confirmedFlag = new Array(size).fill(false)
    const lagMs = []
    let confirmCount = 0
    const tDrainStart = performance.now()

    while (confirmCount + errs < size) {
      if (cancelRef.current) return
      if (performance.now() - tDrainStart > DRAIN_TIMEOUT_MS) break

      const pending = []
      for (let i = 0; i < size; i++) if (!confirmedFlag[i] && submitTs[i] != null) pending.push(i)
      if (pending.length === 0) break

      let p = 0
      const confirmWorker = async () => {
        while (true) {
          if (cancelRef.current) return
          const i = pending[p++]
          if (i === undefined) return
          try {
            if (dataType === 'hash') {
              const c = await api.getHash(remote, `${prefix}.${i}`)
              if (c && c.exists && c.fields && c.fields[`field_${i}`] && String(c.fields[`field_${i}`].origin || '').toLowerCase() === source) {
                confirmedFlag[i] = true
                confirmCount += 1
                lagMs.push(Date.now() - submitTs[i])
                setConfirmed(confirmCount)
              }
            } else if (dataType === 'zset') {
              const c = await api.getZSet(remote, `${prefix}.${i}`)
              if (c && c.exists && c.members && c.members.length > 0 && String(c.members[0].origin || '').toLowerCase() === source) {
                confirmedFlag[i] = true
                confirmCount += 1
                lagMs.push(Date.now() - submitTs[i])
                setConfirmed(confirmCount)
              }
            } else {
              const c = await api.compare(remote, `${prefix}.${i}`)
              if (c && c.exists && String(c.origin || '').toLowerCase() === source) {
                confirmedFlag[i] = true
                confirmCount += 1
                lagMs.push(Date.now() - submitTs[i])
                setConfirmed(confirmCount)
              }
            }
          } catch { /* transient compare error; retry next round */ }
        }
      }
      await Promise.all(Array.from({ length: Math.min(CONFIRM_CONCURRENCY, pending.length) }, confirmWorker))
      if (confirmCount + errs < size) await sleep(DRAIN_POLL_MS)
    }

    const tEnd = performance.now()
    let qAfter = null
    try { qAfter = await api.queueStatus(remote) } catch { /* best-effort */ }

    const finished = confirmCount + errs >= size
    const avgLag = lagMs.length ? Math.round(lagMs.reduce((a, b) => a + b, 0) / lagMs.length) : null
    const minLag = lagMs.length ? Math.round(Math.min(...lagMs)) : null
    const maxLag = lagMs.length ? Math.round(Math.max(...lagMs)) : null
    const replayDelta = (qBefore && qAfter)
      ? (qAfter.metrics.replaySuccess - qBefore.metrics.replaySuccess)
      : null

    const result = {
      id,
      ts: Date.now(),
      source,
      remote,
      size,
      mode,
      dataType,
      submitDurationMs: Math.round(tSubmitEnd - t0),
      drainDurationMs: Math.round(tEnd - tSubmitEnd),
      totalDurationMs: Math.round(tEnd - t0),
      confirmed: confirmCount,
      submitErrors: errs,
      avgLagMs: avgLag,
      minLagMs: minLag,
      maxLagMs: maxLag,
      replayDelta,
      status: finished ? (errs === 0 ? 'done' : 'done-errors') : 'timeout',
    }
    setResults((prev) => [result, ...prev])
    setPhase(result.status)
    setRunning(false)
  }

  function reset() {
    cancelRef.current = true
    setRunning(false)
    setPhase('idle')
    setSubmitted(0)
    setConfirmed(0)
    setSubmitErrors(0)
    setResults([])
  }

  const progressPct = size
    ? Math.round(((phase === 'submitting' ? submitted : confirmed) / size) * 100)
    : 0

  return (
    <Panel
      title="非同步更新壓力測試"
      en="Async Update Stress"
      right={<span className="meta-pill">Session / Hash / ZSet</span>}
    >
      <div className="stack">
        <div className="form-row form-row--4">
          <div className="field">
            <label className="field__label">資料結構 type</label>
            <Segmented
              value={dataType}
              onChange={setDataType}
              options={[
                { value: 'session', label: 'Session' },
                { value: 'hash', label: 'Hash' },
                { value: 'zset', label: 'ZSet' },
              ]}
            />
          </div>
          <div className="field">
            <label className="field__label">寫入來源 write to</label>
            <Segmented
              value={source}
              onChange={setSource}
              options={[
                { value: 'cloud', label: 'Cloud' },
                { value: 'onprem', label: 'OnPrem' },
              ]}
            />
          </div>
          <div className="field">
            <label className="field__label">更新筆數 updates</label>
            <Segmented
              value={size}
              onChange={setSize}
              options={SIZES.map((s) => ({ value: s, label: String(s) }))}
            />
          </div>
          <div className="field">
            <label className="field__label">送出模式 mode</label>
            <Segmented
              value={mode}
              onChange={setMode}
              options={[
                { value: 'sequential', label: '循序 seq' },
                { value: 'concurrent', label: '並行 conc' },
              ]}
            />
          </div>
        </div>

        <div className="row row--between row--wrap">
          <div className="row">
            <Btn variant="primary" disabled={running} onClick={run}>
              {running ? '執行中…' : `執行 Run (${dataType} × ${size})`}
            </Btn>
            <Btn variant="ghost" onClick={reset}>清除 Reset</Btn>
          </div>
          <span className="faint mono" style={{ fontSize: 11 }}>
            {running
              ? `${PHASE_LABEL[phase]} · submitted ${submitted}/${size} · confirmed ${confirmed}/${size} · errors ${submitErrors}`
              : `寫入 ${dataType} 至 ${source} → 非同步同步至 ${remote}`}
          </span>
        </div>

        {(running || phase !== 'idle') && (
          <div className="progress" title={`${confirmed}/${size} confirmed`}>
            <div className="progress__bar" style={{ width: `${progressPct}%` }} />
          </div>
        )}

        {phase === 'timeout' && (
          <Alert kind="warn">
            同步逾時：僅 {confirmed}/{size} 筆在 {Math.round(DRAIN_TIMEOUT_MS / 1000)}s 內於 {remote} 確認
            {submitErrors ? `（另有 ${submitErrors} 筆寫入失敗）` : ''}。請檢查後端佇列或加大 timeout。
          </Alert>
        )}

        {results.length > 0 && (
          <div className="stack">
            <div className="panel__title" style={{ fontSize: 12 }}>
              結果 results
              <span className="en">same mode + env, compare 10 / 100 / 1000</span>
            </div>
            <div className="stress-tbl-wrap">
              <table className="stress-tbl">
                <thead>
                  <tr>
                    <th>時間</th>
                    <th>型態</th>
                    <th>來源→對端</th>
                    <th>筆數</th>
                    <th>模式</th>
                    <th>寫入 submit</th>
                    <th>同步 drain</th>
                    <th>總計 total</th>
                    <th>確認</th>
                    <th>lag 平均/最大</th>
                    <th>replayΔ</th>
                  </tr>
                </thead>
                <tbody>
                  {results.map((r) => (
                    <tr key={r.id}>
                      <td className="mono">{fmtTime(r.ts)}</td>
                      <td className="mono" style={{ color: 'var(--accent)' }}>{r.dataType}</td>
                      <td className="mono">{r.source} → {r.remote}</td>
                      <td className="mono">{r.size}</td>
                      <td className="mono">{r.mode === 'sequential' ? 'seq' : 'conc'}</td>
                      <td className="mono">{fmtMs(r.submitDurationMs)}</td>
                      <td className="mono stress-tbl__drain">{fmtMs(r.drainDurationMs)}</td>
                      <td className="mono">{fmtMs(r.totalDurationMs)}</td>
                      <td className="mono">
                        {r.confirmed}/{r.size}
                        {r.submitErrors ? <span className="stress-tbl__err"> ({r.submitErrors} err)</span> : null}
                      </td>
                      <td className="mono">
                        {r.avgLagMs != null ? `${fmtMs(r.avgLagMs)} / ${fmtMs(r.maxLagMs)}` : '—'}
                      </td>
                      <td className="mono">{r.replayDelta != null ? fmtNum(r.replayDelta) : '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="faint mono" style={{ fontSize: 11 }}>
              submit = 送出全部寫入耗時（同步）；drain = 對端確認全部到達耗時（非同步傳播，主要量測值）；
              lag = 每筆「送出→對端確認」延遲；replayΔ = 對端佇列 replaySuccess 增量（cross-check）。
            </div>
          </div>
        )}
      </div>
    </Panel>
  )
}
