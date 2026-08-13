// Small shared UI primitives — all styled via tokens, no inline magic numbers.
import React from 'react'

export function Panel({ title, en, accent, right, children, bodyClass = '', bodyStyle }) {
  return (
    <section className={`panel ${accent ? `panel--${accent}` : ''}`}>
      <header className="panel__head">
        <div className="panel__title">
          {title}
          {en ? <span className="en">{en}</span> : null}
        </div>
        {right}
      </header>
      <div className={`panel__body ${bodyClass}`} style={bodyStyle}>{children}</div>
    </section>
  )
}

export function RegionTag({ region }) {
  return (
    <span className={`tag tag--${region}`}>
      <span className="tag__dot" />
      {region === 'cloud' ? 'Cloud' : 'OnPrem'}
    </span>
  )
}

export function StatusDot({ status }) {
  const up = status === 'UP'
  const down = status && status !== 'UP'
  const cls = up ? 'dot--up dot--pulse' : down ? 'dot--down' : 'dot--unknown'
  return <span className={`dot ${cls}`} title={status || 'unknown'} />
}

export function Alert({ kind = 'bad', children }) {
  return <div className={`alert alert--${kind}`}>{children}</div>
}

export function Btn({ variant = '', size = '', block = false, disabled, onClick, children, title }) {
  const cls = ['btn']
  if (variant) cls.push(`btn--${variant}`)
  if (size) cls.push(`btn--${size}`)
  if (block) cls.push('btn--block')
  return (
    <button className={cls.join(' ')} disabled={disabled} onClick={onClick} title={title}>
      {children}
    </button>
  )
}

export function Segmented({ options, value, onChange }) {
  return (
    <div className="seg">
      {options.map((o) => {
        const active = o.value === value
        const cls = ['seg__opt']
        if (active) {
          cls.push('seg__opt--active')
          cls.push(`seg__opt--active-${o.value}`)
        }
        return (
          <button
            key={o.value}
            className={cls.join(' ')}
            onClick={() => onChange(o.value)}
            type="button"
          >
            {o.label}
          </button>
        )
      })}
    </div>
  )
}

export function fmtNum(n) {
  if (n === null || n === undefined) return '—'
  if (typeof n !== 'number') return String(n)
  return n.toLocaleString('en-US')
}

export function fmtMs(ms) {
  if (ms === null || ms === undefined) return '—'
  const n = Number(ms)
  if (!Number.isFinite(n)) return '—'
  if (n < 1000) return `${Math.round(n)} ms`
  return `${(n / 1000).toFixed(2)} s`
}

export function fmtTime(t) {
  if (!t) return '—'
  // writtenAt may be epoch millis or ISO string
  const d = typeof t === 'number' ? new Date(t) : new Date(t)
  if (isNaN(d.getTime())) return String(t)
  return d.toLocaleTimeString('zh-Hant', { hour12: false }) +
    '.' + String(d.getMilliseconds()).padStart(3, '0')
}