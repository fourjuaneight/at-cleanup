const BASE = '/repo'

export interface CleanupRequest {
  identifier: string
  app_password: string
  cleanup_types: string[]
  delete_until_days_ago: number
  actually_delete_stuff: boolean
}

export interface CleanupResult {
  num_enqueued: number
  dry_run: boolean
  message: string
  job_id?: string
}

export interface JobStatus {
  job_id: string
  repo: string
  cleanup_types: string[]
  delete_older_than: string
  num_deleted: number
  num_deleted_today: number
  est_num_remaining: number
  job_state: string
  created_at: string
  updated_at: string
  last_deleted_at: string | null
}

async function handleResponse<T>(res: Response): Promise<T> {
  const data = await res.json()
  if (!res.ok) {
    throw new Error((data as { error?: string }).error ?? `HTTP ${res.status}`)
  }
  return data as T
}

export async function submitCleanup(req: CleanupRequest): Promise<CleanupResult> {
  const res = await fetch(`${BASE}/cleanup`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req),
  })
  return handleResponse<CleanupResult>(res)
}

export async function getJobStatus(jobId: string): Promise<JobStatus> {
  const res = await fetch(`${BASE}/cleanup?job_id=${encodeURIComponent(jobId)}`)
  return handleResponse<JobStatus>(res)
}

export async function cancelJob(jobId: string): Promise<{ message: string }> {
  const res = await fetch(`${BASE}/cleanup?job_id=${encodeURIComponent(jobId)}`, {
    method: 'DELETE',
  })
  return handleResponse<{ message: string }>(res)
}
