import React from 'react'
import { Panel, RegionTag, Alert, fmtNum } from './ui.jsx'
import { api, usePoll } from '../api.js'

const POLL_MS = 3000

const METRICS = [
  { k: 'sent', label: 'Sent', tone: 'info' },
  { k: 'sendFail', label: 'SendFail', tone: 'bad' },
  { k: 'consumed', label: 'Consumed', tone: 'info' },
  { k: 'replaySuccess', label: 'ReplayOK', tone: 'ok' },
  { k: 'replayFail', label: 'ReplayFail', tone: 'bad' },
  { k: 'inFlight', label: 'InFlight', tone: 'warn' },
]

function QueueCol({ region }) {
  const { data, error } = usePoll(() => api.queueStatus(region), POLL_MS)
  const m = (data && data.metrics) || {}

  return (
    <div className={`cmp__col cmp__col--${region}`}>
      <div className="cmp__colhead">
        <RegionTag region={region} />
        <span className="faint mono" style={{ fontSize: 11 }}>
          {data ? `${data.location || ''} · ${data.role || ''}` : 'queue-status'}
        </span>
      </div>
      {error && !data ? (
        <Alert kind="bad">{error.message}</Alert>
      ) : !data ? (
        <div className="skeleton" style={{ height: 120 }} />
      ) : (
        <>
          <div className="stat-grid">
            {METRICS.map((mm) => (
              <div className="stat" key={mm.k}>
                <div className="stat__k">{mm.label}</div>
                <div className={`stat__v stat__v--${mm.tone}`}>{fmtNum(m[mm.k])}</div>
              </div>
            ))}
          </div>
          <div className="row row--between" style={{ marginTop: 'var(--sp-3)' }}>
            <span className="kv__k">backlog</span>
            <span className="kv__v" style={{ color: (m.backlog || 0) > 0 ? 'var(--warn)' : 'var(--text-2)' }}>
              {fmtNum(m.backlog)}
            </span>
          </div>
        </>
      )}
    </div>
  )
}

export default function QueuePanel() {
  return (
    <Panel
      title="佇列指標"
      en="Queue Metrics"
      right={<span className="meta-pill">poll 3s</span>}
    >
      <div className="cmp">
        <QueueCol region="cloud" />
        <QueueCol region="onprem" />
      </div>
    </Panel>
  )
}