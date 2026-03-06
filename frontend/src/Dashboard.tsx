import { useState, useEffect } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
)

interface ScoreBucket {
  bucket: string
  count: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface PassRate {
  task: string
  avg_score: number
  attempts: number
}

export function Dashboard() {
  const [lab, setLab] = useState('lab-04')
  const [scores, setScores] = useState<ScoreBucket[]>([])
  const [timeline, setTimeline] = useState<TimelineEntry[]>([])
  const [passRates, setPassRates] = useState<PassRate[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const token = localStorage.getItem('api_key')

  useEffect(() => {
    if (!token) return

    setLoading(true)
    setError('')

    const headers = { Authorization: `Bearer ${token}` }

    Promise.all([
      fetch(`/analytics/scores?lab=${lab}`, { headers }).then(res => {
        if (!res.ok) throw new Error(`Scores: HTTP ${res.status}`)
        return res.json()
      }),
      fetch(`/analytics/timeline?lab=${lab}`, { headers }).then(res => {
        if (!res.ok) throw new Error(`Timeline: HTTP ${res.status}`)
        return res.json()
      }),
      fetch(`/analytics/pass-rates?lab=${lab}`, { headers }).then(res => {
        if (!res.ok) throw new Error(`Pass Rates: HTTP ${res.status}`)
        return res.json()
      }),
    ])
      .then(([scoresData, timelineData, passRatesData]) => {
        setScores(scoresData as ScoreBucket[])
        setTimeline(timelineData as TimelineEntry[])
        setPassRates(passRatesData as PassRate[])
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [lab, token])

  const scoreChartData = {
    labels: scores.map(s => s.bucket),
    datasets: [
      {
        label: 'Score Distribution',
        data: scores.map(s => s.count),
        backgroundColor: 'rgba(75, 192, 192, 0.6)',
      },
    ],
  }

  const timelineChartData = {
    labels: timeline.map(t => t.date),
    datasets: [
      {
        label: 'Submissions',
        data: timeline.map(t => t.submissions),
        borderColor: 'rgb(255, 99, 132)',
        backgroundColor: 'rgba(255, 99, 132, 0.5)',
      },
    ],
  }

  return (
    <div className="dashboard" style={{ padding: '20px' }}>
      <div className="controls" style={{ marginBottom: '20px' }}>
        <label>Select Lab: </label>
        <select value={lab} onChange={e => setLab(e.target.value)}>
          <option value="lab-01">Lab 01</option>
          <option value="lab-02">Lab 02</option>
          <option value="lab-03">Lab 03</option>
          <option value="lab-04">Lab 04</option>
          <option value="lab-05">Lab 05</option>
        </select>
      </div>

      {loading && <p>Loading charts...</p>}
      {error && <p className="error" style={{ color: 'red' }}>Error: {error}</p>}

      <div className="charts-grid" style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '20px', marginBottom: '40px' }}>
        <div className="chart-container" style={{ background: '#f9f9f9', padding: '15px', borderRadius: '8px' }}>
          <h3>Score Distribution</h3>
          <Bar data={scoreChartData} />
        </div>
        <div className="chart-container" style={{ background: '#f9f9f9', padding: '15px', borderRadius: '8px' }}>
          <h3>Submission Timeline</h3>
          <Line data={timelineChartData} />
        </div>
      </div>

      <div className="table-container">
        <h3>Pass Rates</h3>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr style={{ textAlign: 'left', borderBottom: '2px solid #ddd' }}>
              <th style={{ padding: '10px' }}>Task</th>
              <th style={{ padding: '10px' }}>Avg Score</th>
              <th style={{ padding: '10px' }}>Attempts</th>
            </tr>
          </thead>
          <tbody>
            {passRates.map((pr, i) => (
              <tr key={i} style={{ borderBottom: '1px solid #eee' }}>
                <td style={{ padding: '10px' }}>{pr.task}</td>
                <td style={{ padding: '10px' }}>{pr.avg_score}</td>
                <td style={{ padding: '10px' }}>{pr.attempts}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
