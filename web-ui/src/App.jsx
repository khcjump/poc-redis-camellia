import React, { useState } from 'react'
import { Panel, RegionTag, StatusDot, Alert, Btn, Segmented, fmtMs } from './components/ui.jsx'
import { api, usePoll, ApiError } from './api.js'
import StatusBar from './components/StatusBar.jsx'
import WriteSession from './components/WriteSession.jsx'
import SwitchPanel from './components/SwitchPanel.jsx'
import ComparePanel from './components/ComparePanel.jsx'
import QueuePanel from './components/QueuePanel.jsx'
import StressPanel from './components/StressPanel.jsx'
import ParamsPanel from './components/ParamsPanel.jsx'

export default function App() {
  const [lastLatency, setLastLatency] = useState(null)

  return (
    <div className="app">
      <div className="app__inner">
        <header className="app__header">
          <div className="brand">
            <div className="brand__mark" />
            <div>
              <div className="brand__title">Camellia Sync Console</div>
              <div className="brand__sub">雲地 Redis 資料同步控制台</div>
            </div>
          </div>
          <div className="header__meta">
            <span className="meta-pill">proxy <b>/cloud</b> <b>/onprem</b></span>
            <span className="meta-pill">port <b>3000</b></span>
          </div>
        </header>

        {/* 1. Top status bar */}
        <div className="spacer" />
        <StatusBar />

        <div className="spacer" />

        {/* 2. Write session + 3. Switch panel */}
        <div className="grid">
          <WriteSession />
          <SwitchPanel />
        </div>

        <div className="spacer" />

        {/* 4. Compare panel (full width) */}
        <ComparePanel onLatency={setLastLatency} />

        <div className="spacer" />

        {/* 5. Queue metrics (two columns) */}
        <QueuePanel />

        <div className="spacer" />

        {/* 6. Async update stress test (full width) */}
        <div className="spacer" />
        <StressPanel />

        <div className="spacer" />

        {/* 7. Params (two columns) */}
        <ParamsPanel />

        <footer className="app__footer">
          <span>camellia-sync · demo console · same-origin reverse proxy (no CORS)</span>
          <span>同步延遲 readout: {lastLatency != null ? fmtMs(lastLatency) : '—'}</span>
        </footer>
      </div>
    </div>
  )
}