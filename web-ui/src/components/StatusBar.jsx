import React from 'react'
import { Panel, RegionTag, StatusDot, Alert } from './ui.jsx'
import { usePoll, api } from '../api.js'

const POLL_MS = 3000

function RegionHealthCard({ region }) {
  const { data, error, loading } = usePoll(() => api.health(region), POLL_MS)
  const up = data && data.status === 'UP'
  const down = error || (data && data.status !== 'UP')

  return (
    <div className={`panel panel--${region}`} style={{ flex: 1 }}>
      <header className="panel__head">
        <div className="row">
          <StatusDot status={data ? data.status : null} />
          <RegionTag region={region} />
          <span className="panel__title" style={{ marginLeft: 4 }}>
            {region === 'cloud' ? '雲端' : '地端'}
            <span className="en">health</span>
          </span>
        </div>
        <span className={`meta-pill`} style={up ? { color: 'var(--ok)', borderColor: 'var(--ok-line)' } : down ? { color: 'var(--bad)', borderColor: 'var(--bad-line)' } : undefined}>
          {loading && !data ? 'connecting…' : up ? 'UP' : 'DOWN'}
        </span>
      </header>
      <div className="panel__body">
        {error && !data ? (
          <Alert kind="bad">後端無法連線：{error.message}</Alert>
        ) : !data ? (
          <div className="skeleton" style={{ height: 64 }} />
        ) : (
          <div className="kv">
            <span className="kv__k">role</span>
            <span className="kv__v">{data.role || '—'}</span>
            <span className="kv__k">location</span>
            <span className="kv__v">{data.location || '—'}</span>
            <span className="kv__k">redisMode</span>
            <span className="kv__v">{data.redisMode || '—'}</span>
            <span className="kv__k">queue</span>
            <span className="kv__v">
              {data.producerQueueType || '—'} <span className="faint">→</span> {data.consumerQueueType || '—'}
            </span>
          </div>
        )}
      </div>
    </div>
  )
}

export default function StatusBar() {
  return (
    <Panel
      title="區域健康狀態"
      en="Region Health"
      right={<span className="meta-pill">poll 3s</span>}
      bodyClass=""
      bodyStyle={{ padding: 0 }}
    >
      <div className="row" style={{ padding: 'var(--sp-3)', gap: 'var(--sp-3)' }}>
        <RegionHealthCard region="cloud" />
        <RegionHealthCard region="onprem" />
      </div>
    </Panel>
  )
}