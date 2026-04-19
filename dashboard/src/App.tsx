import { useState, useEffect, useRef } from 'react'
import * as api from './api'

type CleanupType = 'post' | 'post_with_media' | 'repost' | 'like'

const CLEANUP_OPTIONS: { id: CleanupType; label: string }[] = [
  { id: 'post', label: 'Posts without Images' },
  { id: 'post_with_media', label: 'Posts with Images' },
  { id: 'repost', label: 'Reposts' },
  { id: 'like', label: 'Likes' },
]

const STATE_COLORS: Record<string, string> = {
  running: 'text-indigo-400',
  completed: 'text-green-400',
  cancelled: 'text-yellow-400',
}

export default function App() {
  const [identifier, setIdentifier] = useState('')
  const [appPassword, setAppPassword] = useState('')
  const [cleanupTypes, setCleanupTypes] = useState<CleanupType[]>([
    'post', 'post_with_media', 'repost', 'like',
  ])
  const [daysAgo, setDaysAgo] = useState(30)

  const [loading, setLoading] = useState(false)
  const [previewResult, setPreviewResult] = useState<api.CleanupResult | null>(null)
  const [submitResult, setSubmitResult] = useState<api.CleanupResult | null>(null)
  const [formError, setFormError] = useState<string | null>(null)

  const [jobId, setJobId] = useState('')
  const [jobStatus, setJobStatus] = useState<api.JobStatus | null>(null)
  const [statusLoading, setStatusLoading] = useState(false)
  const [cancelLoading, setCancelLoading] = useState(false)
  const [statusError, setStatusError] = useState<string | null>(null)

  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const toggleType = (type: CleanupType) => {
    setCleanupTypes(prev =>
      prev.includes(type) ? prev.filter(t => t !== type) : [...prev, type],
    )
  }

  const clearResults = () => {
    setFormError(null)
    setPreviewResult(null)
    setSubmitResult(null)
  }

  const handlePreview = async () => {
    clearResults()
    setLoading(true)
    try {
      const result = await api.submitCleanup({
        identifier,
        app_password: appPassword,
        cleanup_types: cleanupTypes,
        delete_until_days_ago: daysAgo,
        actually_delete_stuff: false,
      })
      setPreviewResult(result)
    } catch (e) {
      setFormError(e instanceof Error ? e.message : 'Unknown error')
    } finally {
      setLoading(false)
    }
  }

  const handleSubmit = async () => {
    clearResults()
    setLoading(true)
    try {
      const result = await api.submitCleanup({
        identifier,
        app_password: appPassword,
        cleanup_types: cleanupTypes,
        delete_until_days_ago: daysAgo,
        actually_delete_stuff: true,
      })
      setSubmitResult(result)
      if (result.job_id) {
        setJobId(result.job_id)
        fetchStatus(result.job_id)
      }
    } catch (e) {
      setFormError(e instanceof Error ? e.message : 'Unknown error')
    } finally {
      setLoading(false)
    }
  }

  const fetchStatus = async (id: string) => {
    setStatusLoading(true)
    setStatusError(null)
    try {
      const result = await api.getJobStatus(id)
      setJobStatus(result)
    } catch (e) {
      setStatusError(e instanceof Error ? e.message : 'Unknown error')
    } finally {
      setStatusLoading(false)
    }
  }

  const handleCheckStatus = () => { if (jobId) fetchStatus(jobId) }

  const handleCancel = async () => {
    if (!jobId) return
    setCancelLoading(true)
    setStatusError(null)
    try {
      await api.cancelJob(jobId)
      await fetchStatus(jobId)
    } catch (e) {
      setStatusError(e instanceof Error ? e.message : 'Unknown error')
    } finally {
      setCancelLoading(false)
    }
  }

  useEffect(() => {
    if (pollRef.current) clearInterval(pollRef.current)
    if (jobStatus?.job_state === 'running' && jobId) {
      pollRef.current = setInterval(() => fetchStatus(jobId), 5000)
    }
    return () => { if (pollRef.current) clearInterval(pollRef.current) }
  }, [jobStatus?.job_state, jobId])

  const formDisabled = loading || !identifier || !appPassword || cleanupTypes.length === 0

  return (
    <div className="min-h-screen bg-gray-950 text-white flex flex-col items-center py-12 px-4">
      <div className="w-full max-w-xl space-y-6">

        {/* Header */}
        <div className="text-center">
          <h1 className="text-4xl font-bold mb-2">Profile Cleaner</h1>
          <p className="text-gray-400">
            A tool to clean up old posts, likes, and reposts from your Bluesky account
          </p>
        </div>

        {/* Credentials */}
        <div className="space-y-3">
          <input
            type="text"
            placeholder="Enter a Bluesky Email, Handle, or DID"
            value={identifier}
            onChange={e => setIdentifier(e.target.value)}
            className="w-full bg-gray-800 border border-gray-700 rounded-lg px-4 py-3 placeholder-gray-500 focus:outline-none focus:border-indigo-500 transition-colors"
          />
          <div>
            <input
              type="password"
              placeholder="Enter your auto-generated App Password"
              value={appPassword}
              onChange={e => setAppPassword(e.target.value)}
              className="w-full bg-gray-800 border border-gray-700 rounded-lg px-4 py-3 placeholder-gray-500 focus:outline-none focus:border-indigo-500 transition-colors"
            />
            <p className="text-sm text-gray-500 mt-1.5">
              Create a single-purpose{' '}
              <a
                href="https://bsky.app/settings/app-passwords"
                target="_blank"
                rel="noopener noreferrer"
                className="text-indigo-400 hover:underline"
              >
                App Password Here
              </a>{' '}
              and delete it once your job is finished running
            </p>
          </div>
        </div>

        {/* Record types */}
        <div>
          <h2 className="font-semibold text-center mb-3">Records to Delete</h2>
          <div className="flex flex-wrap justify-center gap-x-6 gap-y-2">
            {CLEANUP_OPTIONS.map(opt => (
              <label key={opt.id} className="flex items-center gap-2 cursor-pointer select-none">
                <input
                  type="checkbox"
                  checked={cleanupTypes.includes(opt.id)}
                  onChange={() => toggleType(opt.id)}
                  className="w-4 h-4 accent-indigo-500 cursor-pointer"
                />
                <span className="text-sm">{opt.label}</span>
              </label>
            ))}
          </div>
        </div>

        {/* Days */}
        <div className="flex items-center justify-center gap-2">
          <span className="font-semibold">Delete all records older than</span>
          <input
            type="number"
            min={1}
            value={daysAgo}
            onChange={e => setDaysAgo(Math.max(1, Number(e.target.value)))}
            className="w-20 bg-gray-800 border border-gray-700 rounded-lg px-3 py-1.5 text-center focus:outline-none focus:border-indigo-500 transition-colors"
          />
          <span className="font-semibold">days</span>
        </div>

        {/* Info block */}
        <div className="text-center text-sm text-gray-400 space-y-1">
          <p className="font-semibold text-white text-base">Things to Keep in Mind</p>
          <p>Bluesky has creation rate limits of ~1,300 posts/likes/reposts per hour and ~11,000 per day</p>
          <p>Deleting content counts as 1/3 of a post/like/repost</p>
          <p>This tool deletes up to 4,000 records per hour and 30,000 per day</p>
          <p>You should still be able to like/post/repost normally unless you're <em>very</em> active</p>
          <p>If you start getting "Rate Limit" related errors, you can come back and cancel your job</p>
        </div>

        {/* Form feedback */}
        {formError && (
          <div className="bg-red-900/40 border border-red-700 rounded-lg px-4 py-3 text-red-300 text-sm">
            {formError}
          </div>
        )}
        {previewResult && (
          <div className="bg-indigo-900/40 border border-indigo-700 rounded-lg px-4 py-3 text-sm space-y-1">
            <p className="font-semibold">Preview Result</p>
            <p>
              <span className="font-mono text-indigo-300">{previewResult.num_enqueued.toLocaleString()}</span>
              {' '}records would be deleted
            </p>
            {previewResult.message && !previewResult.dry_run && (
              <p className="text-gray-400">{previewResult.message}</p>
            )}
          </div>
        )}
        {submitResult && (
          <div className="bg-green-900/40 border border-green-700 rounded-lg px-4 py-3 text-sm space-y-1">
            <p className="font-semibold">Job Submitted</p>
            <p>{submitResult.message}</p>
            {submitResult.job_id && (
              <p className="text-gray-400 font-mono text-xs break-all">ID: {submitResult.job_id}</p>
            )}
          </div>
        )}

        {/* Action buttons */}
        <div className="flex gap-3 justify-center">
          <button
            onClick={handlePreview}
            disabled={formDisabled}
            className="px-6 py-2.5 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-40 disabled:cursor-not-allowed rounded-lg font-semibold transition-colors"
          >
            {loading ? 'Loading…' : 'Preview'}
          </button>
          <button
            onClick={handleSubmit}
            disabled={formDisabled}
            className="px-6 py-2.5 bg-red-600 hover:bg-red-500 disabled:opacity-40 disabled:cursor-not-allowed rounded-lg font-semibold transition-colors"
          >
            {loading ? 'Loading…' : 'Submit Job'}
          </button>
        </div>

        {/* Job Status */}
        <div className="pt-6 border-t border-gray-800 space-y-4">
          <h2 className="text-xl font-semibold text-center">Job Status</h2>
          <div className="flex flex-wrap justify-center items-center gap-2">
            <input
              type="text"
              placeholder="Existing Job ID"
              value={jobId}
              onChange={e => setJobId(e.target.value)}
              className="flex-2 sm:flex-1 bg-gray-800 border border-gray-700 rounded-lg px-4 py-2.5 placeholder-gray-500 font-mono text-sm focus:outline-none focus:border-indigo-500 transition-colors w-full sm:w-auto"
            />
            <button
              onClick={handleCheckStatus}
              disabled={statusLoading || !jobId}
              className="px-4 py-2.5 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-40 disabled:cursor-not-allowed rounded-lg font-semibold text-sm transition-colors whitespace-nowrap"
            >
              {statusLoading ? 'Checking…' : 'Check Status'}
            </button>
            <button
              onClick={handleCancel}
              disabled={cancelLoading || !jobId}
              className="px-4 py-2.5 bg-red-600 hover:bg-red-500 disabled:opacity-40 disabled:cursor-not-allowed rounded-lg font-semibold text-sm transition-colors whitespace-nowrap"
            >
              {cancelLoading ? 'Cancelling…' : 'Cancel Job'}
            </button>
          </div>

          {statusError && (
            <div className="bg-red-900/40 border border-red-700 rounded-lg px-4 py-3 text-red-300 text-sm">
              {statusError}
            </div>
          )}

          {jobStatus && (
            <div className="bg-gray-800 rounded-lg px-4 py-4 text-sm space-y-2.5">
              <StatusRow label="State">
                <span className={STATE_COLORS[jobStatus.job_state] ?? 'text-red-400'}>
                  {jobStatus.job_state}
                </span>
              </StatusRow>
              <StatusRow label="Records deleted">
                {jobStatus.num_deleted.toLocaleString()}
              </StatusRow>
              <StatusRow label="Est. remaining">
                {jobStatus.est_num_remaining.toLocaleString()}
              </StatusRow>
              <StatusRow label="Deleted today">
                {jobStatus.num_deleted_today.toLocaleString()}
              </StatusRow>
              {jobStatus.last_deleted_at && (
                <StatusRow label="Last activity">
                  {new Date(jobStatus.last_deleted_at).toLocaleString()}
                </StatusRow>
              )}
            </div>
          )}
        </div>

      </div>

      <footer className="mt-12 text-center text-xs text-gray-600">
        This is a straight clone of{' '}
        <a
          href="https://bsky.jazco.dev/cleanup"
          target="_blank"
          rel="noopener noreferrer"
          className="hover:text-gray-400 underline transition-colors"
        >
          Jaz's project
        </a>
        {' '}for my own personal use. Go support them instead.
      </footer>
    </div>
  )
}

function StatusRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex justify-between items-center">
      <span className="text-gray-400">{label}</span>
      <span>{children}</span>
    </div>
  )
}
