import { useEffect, useState } from 'react'
import {
  Bar, BarChart, CartesianGrid, Cell, Legend, Line, LineChart,
  ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts'

const API = '/api'

const RISK = {
  minimal:  'var(--risk-minimal)',
  low:      'var(--risk-low)',
  elevated: 'var(--risk-elevated)',
  high:     'var(--risk-high)',
  critical: 'var(--risk-critical)',
}

const get = (path) =>
  fetch(API + path).then((r) => {
    if (!r.ok) throw new Error(r.status + ' ' + r.statusText)
    return r.json()
  })

const n = (v, d = 0) =>
  v === undefined || v === null ? '—' : Number(v).toLocaleString(undefined, {
    minimumFractionDigits: d, maximumFractionDigits: d,
  })

/* ------------------------------------------------------------------ score */
function ScoreTab() {
  const [form, setForm] = useState({
    amount: 1450, hour: 3, day: 14, month: 6, minute: 20, mcc: 7995,
    merchant_state: 'MEXICO', use_chip: 'Online Transaction',
    device_os: 'Android', error_type: 'Bad PIN', vpn_flag: 1,
    is_online: 1, device_merchant_distance_km: 820, threshold_lift: 30,
  })
  const [result, setResult] = useState(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState(null)

  const set = (k) => (e) => {
    const raw = e.target.value
    const num = e.target.type === 'number' || e.target.dataset.num
    setForm({ ...form, [k]: num ? Number(raw) : raw })
  }

  const score = async () => {
    setBusy(true); setErr(null)
    try {
      const r = await fetch(API + '/predict', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...form, top_n: 8 }),
      })
      if (!r.ok) throw new Error(await r.text())
      setResult(await r.json())
    } catch (e) { setErr(String(e.message || e)) } finally { setBusy(false) }
  }

  useEffect(() => { score() }, [])   // score the seeded example on first load

  const p = result ? result.fraud_probability : 0
  const lift = result ? result.lift_vs_base_rate : 0
  const colour = result ? RISK[result.risk_band] : 'var(--ink-3)'
  const maxShap = result
    ? Math.max(...result.contributions.map((c) => Math.abs(c.shap)), 0.0001) : 1
  // Lift is plotted on a log scale: 1x to 300x spans two and a half decades,
  // and a linear meter would bunch everything at the left.
  const meterPos = result
    ? Math.min(Math.max(Math.log10(Math.max(lift, 0.1)) + 1, 0) / 3.5, 1) * 100 : 0

  return (
    <div className="score-layout">
      <section className="panel">
        <h2>Transaction</h2>
        <div className="body">
          <div className="field-row">
            <label htmlFor="amt">Amount</label>
            <input id="amt" type="number" value={form.amount} onChange={set('amount')} />
          </div>
          <div className="field-grid">
            <div className="field-row">
              <label htmlFor="hr">Hour</label>
              <input id="hr" type="number" min="0" max="23" value={form.hour} onChange={set('hour')} />
            </div>
            <div className="field-row">
              <label htmlFor="dy">Day</label>
              <input id="dy" type="number" min="1" max="31" value={form.day} onChange={set('day')} />
            </div>
          </div>
          <div className="field-row">
            <label htmlFor="mcc">Merchant category (MCC)</label>
            <select id="mcc" value={form.mcc} onChange={set('mcc')} data-num="1">
              <option value="5411">5411 — grocery</option>
              <option value="5812">5812 — restaurant</option>
              <option value="5541">5541 — fuel</option>
              <option value="5912">5912 — pharmacy</option>
              <option value="4829">4829 — money transfer</option>
              <option value="7995">7995 — gambling</option>
            </select>
          </div>
          <div className="field-row">
            <label htmlFor="st">Merchant location</label>
            <select id="st" value={form.merchant_state} onChange={set('merchant_state')}>
              <option>CA</option><option>TX</option><option>NY</option>
              <option>FL</option><option>OH</option>
              <option>MEXICO</option><option>UNKNOWN</option>
            </select>
          </div>
          <div className="field-row">
            <label htmlFor="ch">Entry method</label>
            <select id="ch" value={form.use_chip} onChange={set('use_chip')}>
              <option>Chip Transaction</option>
              <option>Swipe Transaction</option>
              <option>Online Transaction</option>
            </select>
          </div>
          <div className="field-grid">
            <div className="field-row">
              <label htmlFor="os">Device</label>
              <select id="os" value={form.device_os} onChange={set('device_os')}>
                <option>Android</option><option>iOS</option>
                <option>Windows</option><option>macOS</option>
                <option>Linux</option><option>UNKNOWN</option>
              </select>
            </div>
            <div className="field-row">
              <label htmlFor="vpn">VPN in use</label>
              <select id="vpn" value={form.vpn_flag} onChange={set('vpn_flag')} data-num="1">
                <option value="0">No</option><option value="1">Yes</option>
              </select>
            </div>
          </div>
          <div className="field-row">
            <label htmlFor="er">Terminal error</label>
            <select id="er" value={form.error_type} onChange={set('error_type')}>
              <option>NONE</option><option>Bad PIN</option>
              <option>Insufficient Balance</option><option>Bad Card Number</option>
            </select>
          </div>
          <div className="field-row">
            <label htmlFor="km">Device distance from merchant (km)</label>
            <input id="km" type="number" value={form.device_merchant_distance_km}
                   onChange={set('device_merchant_distance_km')} />
          </div>
          <div className="field-row">
            <label htmlFor="thr">Flag when risk exceeds</label>
            <select id="thr" value={form.threshold_lift} onChange={set('threshold_lift')} data-num="1">
              <option value="3">3× baseline — catch more, review more</option>
              <option value="10">10× baseline</option>
              <option value="30">30× baseline — balanced</option>
              <option value="100">100× baseline — high confidence only</option>
            </select>
          </div>
          <button className="primary" onClick={score} disabled={busy}>
            {busy ? 'Scoring…' : 'Score transaction'}
          </button>
        </div>
      </section>

      <section className="panel">
        <h2>Risk assessment</h2>
        <div className="body">
          {err && <div className="msg bad">
            Could not reach the scoring service. Start it with{' '}
            <code>bash scripts/run_api.sh</code>, then score again.<br />{err}
          </div>}

          {!err && !result && <div className="msg wait">Waiting for the first score.</div>}

          {result && (
            <>
              <div className="verdict">
                <div>
                  <div className="label">Risk relative to an average transaction</div>
                  <div className="prob" style={{ color: colour }}>
                    {lift < 10 ? lift.toFixed(1) : Math.round(lift)}
                    <span style={{ fontSize: 28 }}>×</span>
                  </div>
                </div>
                <div>
                  <div className="label">Probability of fraud</div>
                  <div className="secondary" style={{ color: colour }}>
                    {(p * 100).toFixed(3)}%
                  </div>
                  <div className="sublabel">
                    baseline {(result.base_rate * 100).toFixed(3)}%
                  </div>
                </div>
                <div>
                  <div className="label">Flagged above {result.threshold_lift}×</div>
                  <span className="badge" style={{ background: colour }}>
                    {result.verdict} · {result.risk_band}
                  </span>
                  <div className="sublabel">raw model score {result.raw_model_score.toFixed(3)}</div>
                </div>
              </div>

              <div className="meter">
                <div className="track">
                  <div className="needle" style={{ left: `${meterPos}%` }} />
                </div>
                <div className="scale">
                  <span>0.1×</span><span>1×</span><span>10×</span><span>100×</span>
                </div>
              </div>

              <div className="wf">
                <h3>What drove this score</h3>
                <p className="note">
                  Exact TreeSHAP contributions. Bars right of centre push the score
                  up, left push it down. Base value {result.base_value.toFixed(4)}.
                </p>
                {result.contributions.map((c) => {
                  const w = (Math.abs(c.shap) / maxShap) * 50
                  const up = c.shap > 0
                  return (
                    <div className="wf-row" key={c.feature}>
                      <div className="wf-name">
                        {c.feature} <span>= {c.value}</span>
                      </div>
                      <div className="wf-bar">
                        <div className="mid" />
                        <div className="fill" style={{
                          left: up ? '50%' : `${50 - w}%`,
                          width: `${w}%`,
                          background: up ? 'var(--risk-critical)' : 'var(--risk-minimal)',
                        }} />
                      </div>
                      <div className="wf-val" style={{ color: up ? 'var(--risk-critical)' : 'var(--risk-minimal)' }}>
                        {c.shap > 0 ? '+' : ''}{c.shap.toFixed(3)}
                      </div>
                    </div>
                  )
                })}
              </div>

              <div className="caveat">
                <b>Read this alongside the score.</b> {result.note}
              </div>
            </>
          )}
        </div>
      </section>
    </div>
  )
}

/* -------------------------------------------------------------- analytics */
function AnalyticsTab() {
  const [d, setD] = useState({})
  const [err, setErr] = useState(null)

  useEffect(() => {
    Promise.all([
      get('/aggregates/dataset_summary'), get('/aggregates/fraud_by_state'),
      get('/aggregates/fraud_by_mcc'), get('/aggregates/fraud_by_hour'),
      get('/aggregates/amount_distribution'), get('/metrics'),
    ]).then(([summary, state, mcc, hour, amount, metrics]) =>
      setD({ summary, state, mcc, hour, amount, metrics })
    ).catch((e) => setErr(String(e.message || e)))
  }, [])

  if (err) return <div className="msg bad">
    Could not load analytics. Start the API with <code>bash scripts/run_api.sh</code>{' '}
    after running <code>scripts/run_aggregates.sh</code>.<br />{err}
  </div>
  if (!d.summary) return <div className="msg wait">Loading aggregates…</div>

  const s = d.summary
  const bench = d.metrics?.engine_benchmark
  const models = d.metrics?.spark_models?.models || {}
  const tuning = d.metrics?.tuning
  const topStates = (d.state?.rows || []).filter((r) => r.state !== 'UNKNOWN').slice(0, 12)
  const topMcc = (d.mcc?.rows || []).slice(0, 12)
  const hours = (d.hour?.rows || []).map((r) => ({ ...r, hour: Number(r.hour) }))
    .sort((a, b) => a.hour - b.hour)

  const ranked = Object.entries(models)
    .filter(([, m]) => m.pr_auc !== undefined)
    .sort((a, b) => b[1].pr_auc - a[1].pr_auc)

  return (
    <>
      <div className="kpis">
        <div className="kpi"><div className="v">{n(s.total_transactions)}</div><div className="k">transactions analysed</div></div>
        <div className="kpi"><div className="v">{n(s.fraud_transactions)}</div><div className="k">confirmed fraud</div></div>
        <div className="kpi"><div className="v">{s.fraud_rate_pct}%</div><div className="k">base fraud rate</div></div>
        <div className="kpi"><div className="v">{s.raw_size_gb} GB</div><div className="k">raw data in HDFS</div></div>
        <div className="kpi"><div className="v">{n(s.hdfs_blocks)}</div><div className="k">HDFS blocks</div></div>
        <div className="kpi"><div className="v">{n(s.distinct_states)}</div><div className="k">merchant locations</div></div>
      </div>

      <div className="grid">
        <section className="panel">
          <h2>Fraud rate by merchant location</h2>
          <div className="body">
            <ResponsiveContainer width="100%" height={290}>
              <BarChart data={topStates} margin={{ top: 4, right: 8, bottom: 4, left: 4 }}>
                <CartesianGrid strokeDasharray="2 3" stroke="#e3e8f0" vertical={false} />
                <XAxis dataKey="state" tick={{ fontSize: 12, fontFamily: 'IBM Plex Mono' }} />
                <YAxis tick={{ fontSize: 11, fontFamily: 'IBM Plex Mono' }} unit="%" />
                <Tooltip formatter={(v) => v + '%'} />
                <Bar dataKey="fraud_rate_pct" name="fraud rate">
                  {topStates.map((r, i) => (
                    <Cell key={i} fill={r.fraud_rate_pct > 0.3 ? 'var(--risk-critical)'
                      : r.fraud_rate_pct > 0.1 ? 'var(--risk-elevated)' : 'var(--signal)'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </section>

        <section className="panel">
          <h2>Fraud rate through the day</h2>
          <div className="body">
            <ResponsiveContainer width="100%" height={290}>
              <LineChart data={hours} margin={{ top: 4, right: 8, bottom: 4, left: 4 }}>
                <CartesianGrid strokeDasharray="2 3" stroke="#e3e8f0" />
                <XAxis dataKey="hour" tick={{ fontSize: 11, fontFamily: 'IBM Plex Mono' }} />
                <YAxis tick={{ fontSize: 11, fontFamily: 'IBM Plex Mono' }} unit="%" />
                <Tooltip formatter={(v) => v + '%'} labelFormatter={(h) => h + ':00'} />
                <Line type="monotone" dataKey="fraud_rate_pct" name="fraud rate"
                      stroke="var(--risk-high)" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </section>

        <section className="panel">
          <h2>Fraud rate by merchant category</h2>
          <div className="body">
            <ResponsiveContainer width="100%" height={290}>
              <BarChart data={topMcc} layout="vertical" margin={{ top: 4, right: 12, bottom: 4, left: 4 }}>
                <CartesianGrid strokeDasharray="2 3" stroke="#e3e8f0" horizontal={false} />
                <XAxis type="number" tick={{ fontSize: 11, fontFamily: 'IBM Plex Mono' }} unit="%" />
                <YAxis type="category" dataKey="mcc" width={54}
                       tick={{ fontSize: 11, fontFamily: 'IBM Plex Mono' }} />
                <Tooltip formatter={(v) => v + '%'} />
                <Bar dataKey="fraud_rate_pct" name="fraud rate" fill="var(--signal)" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </section>

        <section className="panel">
          <h2>Transaction volume by amount</h2>
          <div className="body">
            <ResponsiveContainer width="100%" height={290}>
              <BarChart data={d.amount?.rows || []} margin={{ top: 4, right: 8, bottom: 4, left: 4 }}>
                <CartesianGrid strokeDasharray="2 3" stroke="#e3e8f0" vertical={false} />
                <XAxis dataKey="bucket" tick={{ fontSize: 11, fontFamily: 'IBM Plex Mono' }} />
                <YAxis tick={{ fontSize: 11, fontFamily: 'IBM Plex Mono' }} />
                <Tooltip formatter={(v, k) => k === 'total_txns' ? n(v) : v + '%'} />
                <Legend wrapperStyle={{ fontSize: 12 }} />
                <Bar dataKey="total_txns" name="transactions" fill="var(--ink-2)" />
                <Bar dataKey="fraud_rate_pct" name="fraud rate %" fill="var(--risk-critical)" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </section>

        <section className="panel">
          <h2>Model comparison, ranked by PR-AUC</h2>
          <div className="body">
            <table className="data">
              <thead><tr>
                <th>Model</th><th className="num">PR-AUC</th><th className="num">ROC-AUC</th>
                <th className="num">Recall</th><th className="num">Train time</th>
              </tr></thead>
              <tbody>
                {ranked.map(([name, m], i) => (
                  <tr key={name} className={i === 0 ? 'best' : ''}>
                    <td className="name">{name.replace(/_/g, ' ')}</td>
                    <td className="num">{m.pr_auc?.toFixed(4)}</td>
                    <td className="num">{m.roc_auc?.toFixed(4)}</td>
                    <td className="num">{m.recall?.toFixed(4)}</td>
                    <td className="num">{n(m.train_seconds)} s</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div className="caveat">
              <b>Why PR-AUC and not accuracy.</b> At a {s.fraud_rate_pct}% base rate, a
              model that never flags anything scores {(100 - s.fraud_rate_pct).toFixed(3)}%
              accuracy while catching zero fraud. ROC-AUC barely separates these models;
              PR-AUC separates them by an order of magnitude.
            </div>
          </div>
        </section>

        <section className="panel">
          <h2>Engine and tuning benchmarks</h2>
          <div className="body">
            {bench && (
              <table className="data">
                <thead><tr><th>Measurement</th><th className="num">Result</th></tr></thead>
                <tbody>
                  <tr><td className="name">Hadoop MapReduce, same aggregation</td><td className="num">{bench.mapreduce_seconds} s</td></tr>
                  <tr><td className="name">Apache Spark, same aggregation</td><td className="num">{bench.spark_seconds} s</td></tr>
                  <tr className="best"><td className="name">Speedup</td><td className="num">{bench.speedup}×</td></tr>
                  <tr><td className="name">MapReduce data spilled to disk</td><td className="num">{(bench.mapreduce_disk_spill_bytes / 1e9).toFixed(2)} GB</td></tr>
                  {tuning && <>
                    <tr><td className="name">Partition pruning</td><td className="num">{tuning.partition_pruning?.speedup}×</td></tr>
                    <tr><td className="name">Broadcast vs shuffle join</td><td className="num">{tuning.join_strategy?.speedup}×</td></tr>
                    <tr><td className="name">Caching on repeat access</td><td className="num">{tuning.caching?.speedup_repeat_access}×</td></tr>
                  </>}
                </tbody>
              </table>
            )}
            <div className="caveat">
              <b>Both engines read the same raw CSV.</b> {bench?.note}
            </div>
          </div>
        </section>
      </div>
    </>
  )
}

/* -------------------------------------------------------------------- app */
export default function App() {
  const [tab, setTab] = useState('score')
  const [health, setHealth] = useState(null)

  useEffect(() => { get('/health').then(setHealth).catch(() => setHealth(null)) }, [])

  return (
    <>
      <header className="masthead">
        <h1>FraudLens</h1>
        <span className="sub">transaction risk console</span>
        <span className="spacer" />
        <span className="stat">
          model <b>{health?.model_loaded ? 'ready' : 'offline'}</b>
          {health?.features ? <> · <b>{health.features}</b> features</> : null}
        </span>
      </header>

      <nav className="tabs" role="tablist">
        <button role="tab" aria-selected={tab === 'score'} onClick={() => setTab('score')}>
          Score a transaction
        </button>
        <button role="tab" aria-selected={tab === 'analytics'} onClick={() => setTab('analytics')}>
          Dataset analytics
        </button>
      </nav>

      <main>{tab === 'score' ? <ScoreTab /> : <AnalyticsTab />}</main>

      <footer className="foot">
        132,500,000 transactions processed on Hadoop HDFS, YARN and Apache Spark.
        Scores are calibrated to the measured base fraud rate.
      </footer>
    </>
  )
}
